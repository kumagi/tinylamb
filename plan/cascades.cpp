/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/string_util.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/query_data.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::cascades {
namespace {

// Practical ceiling for join enumeration. A connected group emits only
// csg-cmp pairs (both sides induced-connected, cut crossed by a conjunct)
// and does so once per group. A disconnected group still walks 2^(n-1)
// bipartitions. Beyond this limit enumeration is skipped and the
// connected-split order built during Memo::Build is the plan.
constexpr size_t kMaxJoinEnumerationRelations = 16;

// constant_unnest_to_values: elements materialized as memo literals beyond
// this count keep the runtime unnest path (memo blow-up and plan-cache
// bloat risk; the executor expands them lazily anyway).
constexpr size_t kConstantUnnestValuesThreshold = 1024;

bool SameColumnReference(const ColumnName& left, const ColumnName& right) {
  if (left.schema.empty() || right.schema.empty()) {
    return left.schema.empty() && right.schema.empty() &&
           IdentifierEquals(left.name, right.name);
  }
  return IdentifierEquals(left.schema, right.schema) &&
         IdentifierEquals(left.name, right.name);
}

std::vector<std::string> Normalize(std::vector<std::string> relations) {
  std::ranges::sort(relations);
  const auto [first, last] = std::ranges::unique(relations);
  relations.erase(first, last);
  return relations;
}

// D5 (docs/design.md) "derived group: fingerprintで識別する": a derived
// group's tag must identify the MEANING of the expressions registered under
// it, not a count.  Two different target lists (different columns, order or
// literals) must never collide into one derived group, and identical ones
// must always reuse it.  Counts and fixed strings are insufficient; hash the
// textual form of every target.
std::string TargetListFingerprint(const std::vector<NamedExpression>& targets) {
  std::string out;
  for (const NamedExpression& target : targets) {
    out += target.name;
    out.push_back('=');
    out += target.expression ? target.expression->ToString() : "<null>";
    out.push_back('|');
  }
  return out;
}

// Identity fragment for derived-group tags (D1: one group, one meaning).
// A tag that names only the rule and a predicate/limit lets two rewrites of
// DIFFERENT input groups over the same relation set -- e.g. the two branches
// or_to_union builds over one shared input -- land in one derived group,
// whose alternatives then mean different row sets; the cost model picks one
// for both consumers and a whole partition silently vanishes. Every tag below
// therefore carries the child group ids it is derived from.
std::string GroupIdsTag(std::initializer_list<GroupId> ids) {
  std::string out = "@";
  for (const GroupId id : ids) {
    out += std::to_string(id);
    out.push_back(',');
  }
  return out;
}

std::string GroupIdsTag(const std::vector<GroupId>& ids) {
  std::string out = "@";
  for (const GroupId id : ids) {
    out += std::to_string(id);
    out.push_back(',');
  }
  return out;
}

std::string PredicateTag(const std::optional<Expression>& predicate) {
  return predicate && *predicate ? (*predicate)->ToString()
                                 : std::string("<none>");
}

std::shared_ptr<AggregateExpression> CloneAggregate(
    const AggregateExpression& source, bool distinct) {
  auto copy = std::make_shared<AggregateExpression>(source.GetType(),
                                                    source.Child(), distinct);
  copy->SetIgnoreNulls(source.IgnoreNulls());
  if (source.Having() != AggregateHavingModifier::kNone) {
    copy->SetHaving(source.Having(), source.HavingCondition());
  }
  copy->SetInnerOrderBy(source.InnerOrderBy());
  copy->SetInnerLimit(source.InnerLimit());
  copy->SetSecondaryArg(source.SecondaryArg());
  copy->SetTrailingArgs(source.TrailingArgs());
  if (source.HasWhereFilter()) {
    copy->SetWhereFilter(source.WhereFilter());
  }
  copy->SetArrayElementSqlType(source.ArrayElementSqlType());
  copy->SetInnerGroupBy(source.InnerGroupBy());
  return copy;
}

std::vector<std::string> UnionRelations(const std::vector<std::string>& left,
                                        const std::vector<std::string>& right) {
  std::vector<std::string> result;
  std::ranges::set_union(left, right, std::back_inserter(result));
  return result;
}

// True when the expression tree carries a window-function call (never
// descending into subquery boundaries). Guards window-eliminating rewrites:
// bypassing a Window node whose results are still referenced would orphan
// `$win` output columns (or evaluate raw calls row-at-a-time, which the
// evaluator rejects).
bool TreeContainsWindowCall(const Expression& expression) {
  if (!expression) {
    return false;
  }
  std::vector<Expression> stack{expression};
  while (!stack.empty()) {
    Expression current = std::move(stack.back());
    stack.pop_back();
    if (!current) {
      continue;
    }
    if (current->Type() == TypeTag::kWindowFunctionExp) {
      return true;
    }
    if (current->Type() == TypeTag::kQueryExp) {
      continue;
    }
    for (const Expression& child : ExpressionChildren(current)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

// Canonical conjunct form: split, sort by printed representation, drop
// duplicates, recombine. Fingerprint stability for payload-bearing
// expressions depends on this.
//
// Detects a provably-empty conjunction: two constraints on the same column
// that no row can satisfy together. Patterns handled:
//   1. col = const AND col IS NULL  (equality contradicts NULL test)
//   2. col > const AND col IS NULL  (range contradicts NULL test)
//   3. col < const AND col IS NULL  (range contradicts NULL test)
//   4. col >= const AND col IS NULL (range contradicts NULL test)
//   5. col <= const AND col IS NULL (range contradicts NULL test)
//   6. col != const AND col = const (equality contradiction)
//   7. col > A AND col < B where A >= B (empty range)
//   8. col < A AND col > B where B >= A (empty range)
//   9. col IS TRUE AND col IS FALSE (a value cannot be both)
// Range reasoning is performed in the constants' own signedness domain;
// a mixed signed/unsigned pair is left to the executor, whose comparison
// is the only authority on whether the operands intersect.
template <typename T>
bool EmptyRangeOrEqualityContradiction(BinaryOperation oi, T a,
                                       BinaryOperation oj, T b) {
  if ((oi == BinaryOperation::kGreaterThan ||
       oi == BinaryOperation::kGreaterThanEquals) &&
      (oj == BinaryOperation::kLessThan ||
       oj == BinaryOperation::kLessThanEquals)) {
    if (a > b || (a == b && (oi == BinaryOperation::kGreaterThan ||
                             oj == BinaryOperation::kLessThan))) {
      return true;
    }
  }
  if ((oi == BinaryOperation::kLessThan ||
       oi == BinaryOperation::kLessThanEquals) &&
      (oj == BinaryOperation::kGreaterThan ||
       oj == BinaryOperation::kGreaterThanEquals)) {
    if (b > a || (b == a && (oj == BinaryOperation::kGreaterThan ||
                             oi == BinaryOperation::kLessThan))) {
      return true;
    }
  }
  if (oi == BinaryOperation::kEquals && oj == BinaryOperation::kEquals &&
      a != b) {
    return true;
  }
  if ((oi == BinaryOperation::kNotEquals && oj == BinaryOperation::kEquals &&
       a == b) ||
      (oj == BinaryOperation::kNotEquals && oi == BinaryOperation::kEquals &&
       a == b)) {
    return true;
  }
  return false;
}

bool ConjunctionIsContradictory(const std::vector<Expression>& conjuncts) {
  struct ColPred {
    std::string col_name;
    BinaryOperation op{};
    Value val;
    bool is_null_test{false};
    bool is_null_positive{false};   // true = IS NULL, false = IS NOT NULL
    bool is_bool_test{false};       // IS TRUE / IS FALSE
    bool is_true_assertion{false};  // true = IS TRUE, false = IS FALSE
  };
  std::vector<ColPred> preds;
  for (const auto& c : conjuncts) {
    if (!c) {
      continue;
    }
    if (c->Type() == TypeTag::kUnaryExp) {
      const auto& unary = c->AsUnaryExpression();
      if ((unary.Op() == UnaryOperation::kIsNull ||
           unary.Op() == UnaryOperation::kIsNotNull) &&
          unary.Child()->Type() == TypeTag::kColumnValue) {
        ColPred p;
        // Qualified name: t1.id and t2.id are different columns.
        p.col_name = unary.Child()->AsColumnValue().GetColumnName().ToString();
        p.is_null_test = true;
        p.is_null_positive = (unary.Op() == UnaryOperation::kIsNull);
        preds.push_back(std::move(p));
      } else if ((unary.Op() == UnaryOperation::kIsTrue ||
                  unary.Op() == UnaryOperation::kIsFalse) &&
                 unary.Child()->Type() == TypeTag::kColumnValue) {
        ColPred p;
        p.col_name = unary.Child()->AsColumnValue().GetColumnName().ToString();
        p.is_bool_test = true;
        p.is_true_assertion = (unary.Op() == UnaryOperation::kIsTrue);
        preds.push_back(std::move(p));
      }
    } else if (c->Type() == TypeTag::kBinaryExp) {
      const auto& bin = c->AsBinaryExpression();
      if (bin.Left()->Type() == TypeTag::kColumnValue &&
          bin.Right()->Type() == TypeTag::kConstantValue) {
        ColPred p;
        p.col_name = bin.Left()->AsColumnValue().GetColumnName().ToString();
        p.op = bin.Op();
        p.val = bin.Right()->AsConstantValue().GetValue();
        preds.push_back(std::move(p));
      } else if (bin.Left()->Type() == TypeTag::kConstantValue &&
                 bin.Right()->Type() == TypeTag::kColumnValue) {
        ColPred p;
        p.col_name = bin.Right()->AsColumnValue().GetColumnName().ToString();
        p.val = bin.Left()->AsConstantValue().GetValue();
        switch (bin.Op()) {
          case BinaryOperation::kLessThan:
            p.op = BinaryOperation::kGreaterThan;
            break;
          case BinaryOperation::kGreaterThan:
            p.op = BinaryOperation::kLessThan;
            break;
          case BinaryOperation::kLessThanEquals:
            p.op = BinaryOperation::kGreaterThanEquals;
            break;
          case BinaryOperation::kGreaterThanEquals:
            p.op = BinaryOperation::kLessThanEquals;
            break;
          default:
            p.op = bin.Op();
            break;
        }
        preds.push_back(std::move(p));
      }
    }
  }
  for (size_t i = 0; i < preds.size(); ++i) {
    for (size_t j = i + 1; j < preds.size(); ++j) {
      if (preds[i].col_name != preds[j].col_name) {
        continue;
      }
      // col IS TRUE AND col IS FALSE -> a value cannot be both.
      if (preds[i].is_bool_test && preds[j].is_bool_test &&
          preds[i].is_true_assertion != preds[j].is_true_assertion) {
        return true;
      }
      // col = N AND col IS NULL -> contradiction
      if (preds[i].is_null_test && preds[i].is_null_positive &&
          !preds[j].is_null_test && !preds[j].is_bool_test &&
          preds[j].op == BinaryOperation::kEquals && !preds[j].val.IsNull()) {
        return true;
      }
      if (preds[j].is_null_test && preds[j].is_null_positive &&
          !preds[i].is_null_test && !preds[i].is_bool_test &&
          preds[i].op == BinaryOperation::kEquals && !preds[i].val.IsNull()) {
        return true;
      }
      if (preds[i].is_null_test || preds[j].is_null_test ||
          preds[i].is_bool_test || preds[j].is_bool_test) {
        continue;
      }
      if (preds[i].val.type == preds[j].val.type &&
          preds[i].val.type == ValueType::kInt64) {
        const auto oi = preds[i].op;
        const auto oj = preds[j].op;
        // UINT64 constants share the INT64 storage with an unsigned tag;
        // their bit patterns are NOT ordered like int64_t (UINT64_MAX
        // reads as -1), so a mixed-signedness pair must not feed the
        // signed range reasoning below.
        if (preds[i].val.IsUnsigned() != preds[j].val.IsUnsigned()) {
          continue;
        }
        bool contradiction = false;
        if (preds[i].val.IsUnsigned()) {
          contradiction = EmptyRangeOrEqualityContradiction(
              oi, static_cast<uint64_t>(preds[i].val.value.int_value), oj,
              static_cast<uint64_t>(preds[j].val.value.int_value));
        } else {
          contradiction = EmptyRangeOrEqualityContradiction(
              oi, preds[i].val.value.int_value, oj,
              preds[j].val.value.int_value);
        }
        if (contradiction) {
          return true;
        }
      }
    }
  }
  return false;
}

// True when `expression` evaluates to FALSE (or NULL) for every row: a
// contradictory conjunction, a constant false, or a disjunction whose every
// branch is always false.
bool ExpressionIsAlwaysFalse(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kConstantValue) {
    const Value value = expression->AsConstantValue().GetValue();
    return value.IsNull() || !value.Truthy();
  }
  if (expression->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  switch (expression->AsBinaryExpression().Op()) {
    case BinaryOperation::kAnd: {
      const std::vector<Expression> conjuncts = SplitConjuncts(expression);
      if (ConjunctionIsContradictory(conjuncts)) {
        return true;
      }
      return std::ranges::any_of(conjuncts, ExpressionIsAlwaysFalse);
    }
    case BinaryOperation::kOr: {
      const std::vector<Expression> disjuncts =
          relational_detail::SplitDisjuncts(expression);
      return !disjuncts.empty() &&
             std::ranges::all_of(disjuncts, ExpressionIsAlwaysFalse);
    }
    default:
      return false;
  }
}

// Structural evaluation-cost class used to order conjuncts (lower runs first).
// The point is short-circuit efficiency: a literal comparison on one column is
// an index-friendly, branch-cheap test, while LIKE, function calls and
// subqueries cost far more per row.  The class is purely structural (no
// statistics), so the canonical order stays deterministic and memo fingerprints
// never depend on catalog state; ToString() still breaks ties, which keeps the
// previous alphabetical order inside one class.
int ConjunctCostClass(const Expression& conjunct) {
  const auto is_ordered_comparison = [](BinaryOperation op) {
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
  };
  if (!conjunct) {
    return 4;
  }
  switch (conjunct->Type()) {
    case TypeTag::kConstantValue:
      return 0;
    case TypeTag::kColumnValue:
      return 1;
    case TypeTag::kUnaryExp:
      switch (conjunct->AsUnaryExpression().Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
          return 2;
        default:
          return 6;
      }
    case TypeTag::kBinaryExp: {
      const BinaryExpression& binary = conjunct->AsBinaryExpression();
      if (binary.Op() == BinaryOperation::kOr) {
        return 7;
      }
      if (binary.Op() == BinaryOperation::kAnd) {
        return 6;
      }
      if (binary.Op() == BinaryOperation::kLike ||
          binary.Op() == BinaryOperation::kNotLike) {
        return 5;
      }
      const bool left_column = binary.Left()->Type() == TypeTag::kColumnValue;
      const bool right_column = binary.Right()->Type() == TypeTag::kColumnValue;
      const bool left_constant =
          binary.Left()->Type() == TypeTag::kConstantValue;
      const bool right_constant =
          binary.Right()->Type() == TypeTag::kConstantValue;
      if ((left_column && right_constant) || (left_constant && right_column)) {
        return is_ordered_comparison(binary.Op()) ? 1 : 3;
      }
      if (left_column && right_column) {
        return is_ordered_comparison(binary.Op()) ? 2 : 4;
      }
      return 4;
    }
    case TypeTag::kInExp:
    case TypeTag::kCastExp:
      return 3;
    case TypeTag::kFunctionCallExp:
      return 5;
    case TypeTag::kCaseExp:
      return 6;
    case TypeTag::kQueryExp:
    case TypeTag::kAggregateExp:
    case TypeTag::kWindowFunctionExp:
      return 8;
    default:
      return 4;
  }
}

Expression CanonicalizeConjuncts(const Expression& predicate) {
  if (!predicate) {
    return nullptr;
  }
  std::vector<Expression> conjuncts = SplitConjuncts(predicate);
  std::ranges::sort(conjuncts, [](const Expression& a, const Expression& b) {
    const int left = ConjunctCostClass(a);
    const int right = ConjunctCostClass(b);
    if (left != right) {
      return left < right;
    }
    return a->ToString() < b->ToString();
  });
  conjuncts.erase(
      std::ranges::unique(conjuncts,
                          [](const Expression& a, const Expression& b) {
                            return a->ToString() == b->ToString();
                          })
          .begin(),
      conjuncts.end());
  // Provably-empty predicate: a contradictory conjunction, an always-false
  // conjunct, or a disjunction whose every branch is contradictory.
  if (ConjunctionIsContradictory(conjuncts) ||
      std::ranges::any_of(conjuncts, ExpressionIsAlwaysFalse)) {
    return ConstantValueExp(Value(false));
  }
  return CombineConjuncts(conjuncts);
}

// True when a Selection living in `selection_group` may strengthen the
// shared single-relation scan groups beneath it. Scan-group filters are
// global (D1: every consumer of the group sees the same rows), so only a
// Selection whose predicate holds for every consumer of the scan may merge
// into them. An or_to_union branch filters a SHARED input with a predicate
// that is only one partition of the OR: merging it would AND every branch's
// partition predicate into the common scan and empty the whole union.
// The same holds for a filter pushed into ONE branch of a set operation
// (push_filter_past_setop): the FULL OUTER JOIN decomposition's left-outer
// branch and its anti-join branch read the same base scans, so a WHERE
// conjunct merged into a scan from the left-outer branch hid rows the anti
// branch needs to decide which right rows are unmatched (spurious NULL-padded
// rows appeared).
// Groups derived from such a Selection inherit the mark (Group::branch_local),
// so the predicate stays branch-local however far it is pushed.
bool SelectionMayStrengthenSharedScans(const Memo& memo,
                                       GroupId selection_group) {
  return !memo.Get(selection_group).branch_local;
}

// Marks `derived` as carrying a branch-local predicate when `source` does.
void InheritBranchLocal(Memo& memo, GroupId derived, GroupId source) {
  if (memo.Get(source).branch_local) {
    memo.Get(derived).branch_local = true;
  }
}

// Pushes every single-relation conjunct of `predicate` into the matching scan
// group's filter. Returns the conjuncts that have no single-relation home and
// therefore must stay in the Selection. Guard rail (Phase 2): nothing here
// reasons about null-rejection, so the rule must never be used to push
// predicates through outer joins once those exist.
// Extract per-table local conditions from an OR predicate that touches
// multiple tables.  For each table involved, collect the conjuncts from
// every OR branch that reference only that table.  If every branch has
// at least one local conjunct for a given table, the local disjunction
// is pushed to that table's scan filter as a selective pre-filter.
//
// Example: OR(AND(t1.a=1, t2.b=2), AND(t1.a=3, t2.b=4))
//   → t1 scan filter: OR(t1.a=1, t1.a=3)
//   → t2 scan filter: OR(t2.b=2, t2.b=4)
void PushOrLocalConditions(
    Memo& memo, const Expression& or_expr,
    const std::function<bool(const std::string&)>& relation_enabled) {
  if (!or_expr || or_expr->Type() != TypeTag::kBinaryExp ||
      or_expr->AsBinaryExpression().Op() != BinaryOperation::kOr) {
    return;
  }
  const std::vector<Expression> branches =
      tinylamb::relational_detail::SplitDisjuncts(or_expr);
  if (branches.size() < 2) {
    return;
  }
  // Collect all table names touched by the OR.
  std::unordered_set<std::string> all_tables;
  for (const Expression& branch : branches) {
    for (const ColumnName& col : branch->TouchedColumns()) {
      if (!col.schema.empty()) {
        all_tables.insert(col.schema);
      }
    }
  }
  // For each table, try to extract local conditions from every branch.
  for (const std::string& table : all_tables) {
    if (!memo.ContainsRelation({table}) || !relation_enabled(table)) {
      continue;
    }
    std::vector<Expression> local_branches;
    local_branches.reserve(branches.size());
    bool every_branch_has_local = true;
    for (const Expression& branch : branches) {
      std::vector<Expression> local;
      for (const Expression& conjunct : SplitConjuncts(branch)) {
        std::unordered_set<std::string> touched;
        for (const ColumnName& col : conjunct->TouchedColumns()) {
          if (!col.schema.empty()) {
            touched.insert(col.schema);
          }
        }
        if (touched.size() == 1 && touched.contains(table)) {
          local.push_back(conjunct);
        }
      }
      if (local.empty()) {
        // This branch has no condition on this table; the local
        // disjunction cannot safely filter (a branch without a
        // condition on this table is always true for this table).
        every_branch_has_local = false;
        break;
      }
      local_branches.push_back(CombineConjuncts(local));
    }
    if (every_branch_has_local && local_branches.size() >= 2) {
      memo.MergeScanFilter(
          memo.EnsureGroup({table}),
          tinylamb::relational_detail::CombineDisjuncts(local_branches));
    }
  }
}

std::vector<Expression> PushSingleRelationConjuncts(
    Memo& memo, const Expression& predicate,
    const std::function<bool(const std::string&)>& relation_enabled) {
  std::vector<Expression> residual;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    std::unordered_set<std::string> touched;
    for (const ColumnName& column : conjunct->TouchedColumns()) {
      if (!column.schema.empty()) {
        touched.insert(column.schema);
      }
    }
    if (touched.size() == 1 && memo.ContainsRelation({*touched.begin()}) &&
        relation_enabled(*touched.begin())) {
      memo.MergeScanFilter(memo.EnsureGroup({*touched.begin()}), conjunct);
      continue;
    }
    // For cross-table OR predicates, extract per-table local conditions
    // and push them to each table's scan filter.
    if (touched.size() > 1 && conjunct->Type() == TypeTag::kBinaryExp &&
        conjunct->AsBinaryExpression().Op() == BinaryOperation::kOr) {
      PushOrLocalConditions(memo, conjunct, relation_enabled);
    }
    residual.push_back(conjunct);
  }
  return residual;
}

bool ContainsAggregate(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression), ContainsAggregate);
}

bool OutputMatchesColumn(const NamedExpression& output,
                         const ColumnName& column) {
  // Qualified-name matching only.  A bare-name fallback (output "a" vs
  // predicate column t2.a) rewrites the predicate onto the FIRST output
  // named "a" -- possibly a different relation's column -- and pushes the
  // filter below the projection against the wrong data.
  if (!output.name.empty() && output.name == column.ToString()) {
    return true;
  }
  if (output.expression && output.expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& source =
        output.expression->AsColumnValue().GetColumnName();
    return source == column;
  }
  return false;
}

std::optional<Expression> RewriteThroughOutputs(
    const Expression& expression,
    const std::vector<NamedExpression>& outputs) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    for (const NamedExpression& output : outputs) {
      if (OutputMatchesColumn(output, column)) {
        return output.expression;
      }
    }
    return std::nullopt;
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  if (children.empty()) {
    return expression;
  }
  std::vector<Expression> rewritten;
  rewritten.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped = RewriteThroughOutputs(child, outputs);
    if (!mapped) {
      return std::nullopt;
    }
    rewritten.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expression, std::move(rewritten));
}

std::vector<NamedExpression> GroupingOutputs(
    const std::vector<NamedExpression>& outputs) {
  std::vector<NamedExpression> grouping;
  for (const NamedExpression& output : outputs) {
    if (!ContainsAggregate(output.expression)) {
      grouping.push_back(output);
    }
  }
  return grouping;
}

Expression EqualityConstantExpr(const Expression& predicate,
                                const ColumnName& column) {
  if (!predicate) {
    return {};
  }
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Op() != BinaryOperation::kEquals) {
      continue;
    }
    if (binary.Left()->Type() == TypeTag::kColumnValue &&
        binary.Right()->Type() == TypeTag::kConstantValue &&
        binary.Left()->AsColumnValue().GetColumnName() == column) {
      return binary.Right();
    }
    if (binary.Right()->Type() == TypeTag::kColumnValue &&
        binary.Left()->Type() == TypeTag::kConstantValue &&
        binary.Right()->AsColumnValue().GetColumnName() == column) {
      return binary.Left();
    }
  }
  return {};
}

void InferJoinConstants(Memo& memo, const Expression& join_predicate) {
  if (!join_predicate) {
    return;
  }
  for (const Expression& conjunct : SplitConjuncts(join_predicate)) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Op() != BinaryOperation::kEquals ||
        binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName left = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName right = binary.Right()->AsColumnValue().GetColumnName();
    const auto push = [&](const ColumnName& from, const ColumnName& to) {
      if (from.schema.empty() || to.schema.empty()) {
        return;
      }
      // A conjunct may reference a table outside this memo's join graph
      // (e.g. an outer reference under Apply); skip the push instead of
      // tripping RelationMask's CHECK.
      if (!memo.ContainsRelation({from.schema, to.schema})) {
        return;
      }
      const Expression constant = EqualityConstantExpr(
          memo.Get(memo.EnsureGroup({from.schema})).filter, from);
      if (!constant) {
        return;
      }
      // Share the original constant node. A ParameterSlot copied via
      // GetValue() would freeze the fill-time literal on the other side.
      memo.MergeScanFilter(
          memo.EnsureGroup({to.schema}),
          BinaryExpressionExp(ColumnValueExp(to), BinaryOperation::kEquals,
                              constant));
    };
    push(left, right);
    push(right, left);
  }
}

Expression BuildEqualityOnAllColumns(const Memo& memo,
                                     const LogicalExpression& expression,
                                     GroupId left_group_id,
                                     GroupId right_group_id) {
  if (expression.predicate && *expression.predicate) {
    return CanonicalizeConjuncts(*expression.predicate);
  }
  const Group& left_group = memo.Get(left_group_id);
  const Group& right_group = memo.Get(right_group_id);
  const std::string left_rel =
      left_group.relations.empty() ? "" : left_group.relations.front();
  const std::string right_rel =
      right_group.relations.empty() ? "" : right_group.relations.front();

  // INTERSECT/EXCEPT compare rows with set semantics: two rows are
  // duplicates when their columns are equal OR both NULL. Plain `=` is
  // null-rejecting, so a null-bearing row could never match its twin and
  // would be dropped from INTERSECT (or wrongly kept by EXCEPT). IS NOT
  // DISTINCT FROM reproduces the set-op comparison exactly, and the
  // implementation rules route it through the null-safe hash-join key path.
  constexpr BinaryOperation kRowEquality = BinaryOperation::kIsNotDistinctFrom;

  std::vector<Expression> conjuncts;

  if (expression.output_schema.ColumnCount() > 0) {
    for (size_t i = 0; i < expression.output_schema.ColumnCount(); ++i) {
      const auto& col = expression.output_schema.GetColumn(i);
      ColumnName left_col = col.Name();
      ColumnName right_col = col.Name();
      if (!left_rel.empty()) {
        left_col.schema = left_rel;
      }
      if (!right_rel.empty()) {
        right_col.schema = right_rel;
      }
      conjuncts.push_back(BinaryExpressionExp(
          ColumnValueExp(left_col), kRowEquality, ColumnValueExp(right_col)));
    }
  } else if (!expression.target_list.empty()) {
    for (const auto& target : expression.target_list) {
      ColumnName left_col(target.name);
      ColumnName right_col(target.name);
      if (target.expression &&
          target.expression->Type() == TypeTag::kColumnValue) {
        left_col = target.expression->AsColumnValue().GetColumnName();
        right_col = left_col;
      }
      if (!left_rel.empty()) {
        left_col.schema = left_rel;
      }
      if (!right_rel.empty()) {
        right_col.schema = right_rel;
      }
      conjuncts.push_back(BinaryExpressionExp(
          ColumnValueExp(left_col), kRowEquality, ColumnValueExp(right_col)));
    }
  } else {
    const auto find_schema_or_targets =
        [](const Group& g) -> std::pair<Schema, std::vector<NamedExpression>> {
      for (const auto& expr : g.expressions) {
        if (expr.output_schema.ColumnCount() > 0) {
          return {expr.output_schema, expr.target_list};
        }
        if (!expr.target_list.empty()) {
          return {expr.output_schema, expr.target_list};
        }
      }
      return {};
    };
    auto [left_sch, left_targets] = find_schema_or_targets(left_group);
    auto [right_sch, right_targets] = find_schema_or_targets(right_group);
    if (!left_targets.empty() && left_targets.size() == right_targets.size()) {
      for (size_t i = 0; i < left_targets.size(); ++i) {
        conjuncts.push_back(BinaryExpressionExp(left_targets[i].expression,
                                                kRowEquality,
                                                right_targets[i].expression));
      }
    } else if (left_sch.ColumnCount() > 0 &&
               left_sch.ColumnCount() == right_sch.ColumnCount()) {
      for (size_t i = 0; i < left_sch.ColumnCount(); ++i) {
        ColumnName left_col = left_sch.GetColumn(i).Name();
        ColumnName right_col = right_sch.GetColumn(i).Name();
        if (left_col.schema.empty() && !left_rel.empty()) {
          left_col.schema = left_rel;
        }
        if (right_col.schema.empty() && !right_rel.empty()) {
          right_col.schema = right_rel;
        }
        conjuncts.push_back(BinaryExpressionExp(
            ColumnValueExp(left_col), kRowEquality, ColumnValueExp(right_col)));
      }
    }
  }

  if (conjuncts.empty()) {
    // No equatable columns are known. A previous revision invented an `id`
    // equality here without checking that such a column exists; that
    // fabricated predicate silently joined on the wrong key (or failed
    // downstream). Return null so callers skip the rewrite instead.
    return memo.JoinConditionFor(left_group, right_group);
  }

  return CanonicalizeConjuncts(CombineConjuncts(conjuncts));
}

bool IsSameTable(const std::string& t1, const std::string& t2) {
  if (t1 == t2) {
    return true;
  }
  if (t1.empty() || t2.empty()) {
    return false;
  }
  auto strip_alias = [](std::string s) {
    const size_t pos = s.rfind('_');
    if (pos != std::string::npos && pos + 1 < s.size()) {
      bool all_digits = true;
      for (size_t i = pos + 1; i < s.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(s[i]))) {
          all_digits = false;
          break;
        }
      }
      if (all_digits) {
        return s.substr(0, pos);
      }
    }
    return s;
  };
  std::string base1 = strip_alias(t1);
  std::string base2 = strip_alias(t2);
  return !base1.empty() && base1 == base2;
}

bool AreFiltersEquivalent(const Expression& f1, const std::string& r1,
                          const Expression& f2, const std::string& r2) {
  if (!f1 && !f2) {
    return true;
  }
  if (!f1 || !f2) {
    return false;
  }
  std::string s1 = f1->ToString();
  std::string s2 = f2->ToString();
  if (s1 == s2) {
    return true;
  }
  auto normalize = [](std::string str, const std::string& rel) {
    if (rel.empty()) {
      return str;
    }
    size_t pos = 0;
    while ((pos = str.find(rel, pos)) != std::string::npos) {
      str.replace(pos, rel.length(), "$T");
      pos += 2;
    }
    return str;
  };
  return normalize(s1, r1) == normalize(s2, r2);
}

std::optional<std::string> SingleUniqueKeyEquality(
    const Memo& memo, const Expression& predicate, const std::string& r1,
    const std::string& r2, const std::string& unique_relation) {
  if (!predicate) {
    return std::nullopt;
  }
  // The predicate must be EXACTLY the key equality: eliminating a join whose
  // condition carries additional conjuncts would drop them.
  const std::vector<Expression> conjuncts = SplitConjuncts(predicate);
  if (conjuncts.size() != 1 || !conjuncts[0] ||
      conjuncts[0]->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  const auto& binary = conjuncts[0]->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kEquals ||
      binary.Left()->Type() != TypeTag::kColumnValue ||
      binary.Right()->Type() != TypeTag::kColumnValue) {
    return std::nullopt;
  }
  const ColumnName& c1 = binary.Left()->AsColumnValue().GetColumnName();
  const ColumnName& c2 = binary.Right()->AsColumnValue().GetColumnName();
  const bool forward = c1.schema == r1 && c2.schema == r2;
  const bool backward = c1.schema == r2 && c2.schema == r1;
  if ((!forward && !backward) || c1.name != c2.name) {
    return std::nullopt;
  }
  // Uniqueness must be proven from the catalog-published schema; a name
  // that merely looks like a key proves nothing.
  const auto& schemas = memo.GetTableSchemas();
  const auto found = schemas.find(unique_relation);
  if (found == schemas.end()) {
    return std::nullopt;
  }
  const int offset = found->second.Offset(ColumnName("", c1.name));
  if (offset < 0) {
    return std::nullopt;
  }
  if (!found->second.GetColumn(static_cast<size_t>(offset))
           .GetConstraint()
           .IsUnique()) {
    return std::nullopt;
  }
  return c1.name;
}

// True when the column is declared NOT NULL (or a primary key) in the
// relation's catalog-published schema.  Unknown schemas refuse.
bool ColumnIsDeclaredNonNull(const Memo& memo, const std::string& relation,
                             const ColumnName& column) {
  const auto& schemas = memo.GetTableSchemas();
  const auto found = schemas.find(relation);
  if (found == schemas.end()) {
    return false;
  }
  const int offset = found->second.Offset(ColumnName("", column.name));
  if (offset < 0) {
    return false;
  }
  const Constraint::ConstraintType ctype =
      found->second.GetColumn(static_cast<size_t>(offset))
          .GetConstraint()
          .ctype;
  return ctype == Constraint::kNotNull || ctype == Constraint::kPrimaryKey;
}

void InferJoinInequalities(Memo& memo, const Expression& join_predicate) {
  if (!join_predicate) {
    return;
  }
  for (const Expression& conjunct : SplitConjuncts(join_predicate)) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Op() != BinaryOperation::kEquals ||
        binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName left = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName right = binary.Right()->AsColumnValue().GetColumnName();
    const auto push_inequalities = [&](const ColumnName& from,
                                       const ColumnName& to) {
      if (from.schema.empty() || to.schema.empty()) {
        return;
      }
      // A conjunct may reference a table outside this memo's join graph
      // (e.g. an outer reference under Apply); skip the push instead of
      // tripping RelationMask's CHECK.
      if (!memo.ContainsRelation({from.schema, to.schema})) {
        return;
      }
      const Expression from_filter =
          memo.Get(memo.EnsureGroup({from.schema})).filter;
      if (!from_filter) {
        return;
      }
      for (const Expression& filter_conjunct : SplitConjuncts(from_filter)) {
        if (!filter_conjunct ||
            filter_conjunct->Type() != TypeTag::kBinaryExp) {
          continue;
        }
        const auto& f_bin = filter_conjunct->AsBinaryExpression();
        if (f_bin.Op() == BinaryOperation::kEquals) {
          continue;
        }
        if (f_bin.Left()->Type() == TypeTag::kColumnValue &&
            f_bin.Right()->Type() == TypeTag::kConstantValue) {
          if (f_bin.Left()->AsColumnValue().GetColumnName() == from) {
            memo.MergeScanFilter(
                memo.EnsureGroup({to.schema}),
                BinaryExpressionExp(ColumnValueExp(to), f_bin.Op(),
                                    f_bin.Right()));
          }
        } else if (f_bin.Right()->Type() == TypeTag::kColumnValue &&
                   f_bin.Left()->Type() == TypeTag::kConstantValue) {
          if (f_bin.Right()->AsColumnValue().GetColumnName() == from) {
            memo.MergeScanFilter(memo.EnsureGroup({to.schema}),
                                 BinaryExpressionExp(f_bin.Left(), f_bin.Op(),
                                                     ColumnValueExp(to)));
          }
        }
      }
    };
    push_inequalities(left, right);
    push_inequalities(right, left);
  }
}

std::pair<std::vector<std::string>, std::vector<std::string>>
GreedyConnectedSplit(const Memo& memo,
                     const std::vector<std::string>& relations) {
  const uint64_t within = memo.RelationMask(relations);
  std::string best_pivot = relations.front();
  double largest = -1.0;
  const bool connected = memo.InducedSubgraphConnected(within);
  for (const std::string& pivot : relations) {
    const uint64_t pivot_mask = memo.RelationMask({pivot});
    // Peel a large peripheral relation so the smaller, filtered relations
    // combine first, without disconnecting the remainder of a connected graph.
    if (connected && !memo.InducedSubgraphConnected(within & ~pivot_mask)) {
      continue;
    }
    const double rows = memo.RelationRows(pivot);
    if (rows > largest) {
      largest = rows;
      best_pivot = pivot;
    }
  }
  std::vector<std::string> left{best_pivot};
  std::vector<std::string> right;
  right.reserve(relations.size() - 1);
  for (const std::string& relation : relations) {
    if (relation != best_pivot) {
      right.push_back(relation);
    }
  }
  return {std::move(left), std::move(right)};
}

// Fingerprints are an internal, locale-independent encoding. Stream sentry
// and locale formatting costs otherwise dominate repeated memo identities for
// large expression trees; append delimiters and decimal integers directly.
class IdentityBuffer {
 public:
  IdentityBuffer& operator<<(std::string_view text) {
    text_.append(text);
    return *this;
  }
  IdentityBuffer& operator<<(const char* text) {
    text_.append(text);
    return *this;
  }
  IdentityBuffer& operator<<(char value) {
    text_.push_back(value);
    return *this;
  }
  IdentityBuffer& operator<<(bool value) {
    return *this << static_cast<unsigned>(value);
  }
  template <std::integral Integer>
  IdentityBuffer& operator<<(Integer value) {
    static_assert(sizeof(Integer) <= sizeof(uint64_t));
    std::array<char, 32> digits{};
    const auto converted =
        std::to_chars(digits.data(), digits.data() + digits.size(), value);
    text_.append(digits.data(),
                 static_cast<size_t>(converted.ptr - digits.data()));
    return *this;
  }
  IdentityBuffer& operator<<(const void* pointer) {
    const auto address = reinterpret_cast<uintptr_t>(pointer);
    if (address != 0) {
      text_.append("0x");
    }
    std::array<char, 2 * sizeof(uintptr_t)> digits{};
    const auto converted = std::to_chars(
        digits.data(), digits.data() + digits.size(), address, 16);
    text_.append(digits.data(),
                 static_cast<size_t>(converted.ptr - digits.data()));
    return *this;
  }
  [[nodiscard]] std::string Take() && { return std::move(text_); }

 private:
  std::string text_;
};

void AppendIdentityString(IdentityBuffer& out, std::string_view text) {
  out << text.size() << ':' << text;
}

void AppendIdentityExpression(IdentityBuffer& out,
                              const Expression& expression);

void AppendIdentityValue(IdentityBuffer& out, const Value& value) {
  out << "v" << static_cast<int>(value.type) << ':'
      << static_cast<unsigned>(value.Collation()) << ':';
  switch (value.type) {
    case ValueType::kNull:
      break;
    case ValueType::kInt64:
    case ValueType::kDate:
      out << value.value.int_value;
      break;
    case ValueType::kDouble:
      out << std::bit_cast<uint64_t>(value.value.double_value);
      break;
    case ValueType::kVarChar:
      AppendIdentityString(out, value.value.varchar_value);
      break;
    case ValueType::kArray: {
      out << value.ArrayElements().size() << ':';
      for (const Value& element : value.ArrayElements()) {
        AppendIdentityValue(out, element);
        out << ';';
      }
      AppendIdentityString(out, value.ArrayElementSqlType());
      break;
    }
  }
}

void AppendIdentityExpression(IdentityBuffer& out,
                              const Expression& expression) {
  if (!expression) {
    out << "<null>";
    return;
  }
  out << "e" << static_cast<int>(expression->Type()) << ':';
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      AppendIdentityString(out, name.schema);
      out << ':';
      AppendIdentityString(out, name.name);
      break;
    }
    case TypeTag::kConstantValue:
      AppendIdentityValue(out, expression->AsConstantValue().GetValue());
      break;
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      out << static_cast<int>(binary.Op()) << ':';
      AppendIdentityExpression(out, binary.Left());
      out << ':';
      AppendIdentityExpression(out, binary.Right());
      break;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      out << static_cast<int>(unary.Op()) << ':';
      AppendIdentityExpression(out, unary.Child());
      break;
    }
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      out << static_cast<int>(aggregate.GetType()) << ':'
          << aggregate.Distinct() << ':' << aggregate.IgnoreNulls() << ':'
          << static_cast<int>(aggregate.Having()) << ':';
      AppendIdentityExpression(out, aggregate.Child());
      out << ':';
      AppendIdentityExpression(out, aggregate.HavingCondition());
      out << ':';
      AppendIdentityExpression(out, aggregate.SecondaryArg());
      out << ':';
      for (const Expression& arg : aggregate.TrailingArgs()) {
        AppendIdentityExpression(out, arg);
        out << ';';
      }
      out << ':';
      AppendIdentityExpression(out, aggregate.WhereFilter());
      out << ':';
      AppendIdentityString(out, aggregate.ArrayElementSqlType());
      out << ':' << aggregate.InnerLimit().has_value() << ':';
      if (aggregate.InnerLimit().has_value()) {
        out << *aggregate.InnerLimit();
      }
      out << ':';
      for (const WindowOrderTerm& term : aggregate.InnerOrderBy()) {
        AppendIdentityExpression(out, term.expression);
        out << ':' << term.ascending << ':' << term.nulls_first.value_or(false)
            << ';';
      }
      out << ':';
      for (const Expression& key : aggregate.InnerGroupBy()) {
        AppendIdentityExpression(out, key);
        out << ';';
      }
      break;
    }
    case TypeTag::kFunctionCallExp: {
      const auto& function = expression->AsFunctionCallExpression();
      AppendIdentityString(out, function.FuncName());
      out << ':' << function.Args().size() << ':';
      for (const Expression& arg : function.Args()) {
        AppendIdentityExpression(out, arg);
        out << ';';
      }
      break;
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      out << static_cast<const void*>(query.Query().get()) << ':'
          << query.Exists() << ':' << query.Negated() << ':'
          << static_cast<int>(query.Op()) << ':'
          << static_cast<int>(query.Mode()) << ':' << query.ArrayResult()
          << ':';
      AppendIdentityString(out, query.ArrayElementSqlType());
      out << ':';
      AppendIdentityExpression(out, query.Test());
      break;
    }
    case TypeTag::kWindowFunctionExp: {
      const auto& window = expression->AsWindowFunctionCallExpression();
      AppendIdentityString(out, window.function);
      out << ':' << window.distinct << ':' << window.respect_nulls << ':'
          << window.ignore_nulls << ':';
      for (const Expression& arg : window.args) {
        AppendIdentityExpression(out, arg);
        out << ';';
      }
      out << ':';
      AppendIdentityExpression(out, window.where_filter);
      out << ':';
      for (const Expression& key : window.partition_by) {
        AppendIdentityExpression(out, key);
        out << ';';
      }
      out << ':';
      for (const WindowOrderTerm& term : window.order_by) {
        AppendIdentityExpression(out, term.expression);
        out << ':' << term.ascending << ':' << term.nulls_first.value_or(false)
            << ';';
      }
      out << ':' << static_cast<int>(window.frame_unit) << ':'
          << static_cast<int>(window.frame_start.type) << ':';
      AppendIdentityExpression(out, window.frame_start.offset);
      out << ':' << static_cast<int>(window.frame_end.type) << ':';
      AppendIdentityExpression(out, window.frame_end.offset);
      out << ':' << window.has_frame << ':'
          << static_cast<int>(window.exclusion) << ':'
          << window.inner_limit.has_value() << ':';
      if (window.inner_limit.has_value()) {
        out << *window.inner_limit;
      }
      break;
    }
    default:
      AppendIdentityString(out, expression->ToString());
      break;
  }
}

std::string ExpressionIdentity(const Expression& expression) {
  IdentityBuffer out;
  AppendIdentityExpression(out, expression);
  return std::move(out).Take();
}

}  // namespace

std::string LogicalExpression::Fingerprint() const {
  std::ostringstream out;
  out << static_cast<int>(operation) << ':' << table;
  for (GroupId child : children) {
    out << ':' << child;
  }
  if (operation == LogicalOperator::kOuterJoin) {
    out << "#j:" << static_cast<unsigned>(join_type);
  }
  if (predicate && *predicate) {
    out << "#p:" << ExpressionIdentity(*predicate);
  }
  for (const NamedExpression& item : target_list) {
    // A target expression can be a null handle when a rule built a
    // placeholder; the fingerprint must not dereference it (SIGSEGV).
    out << "#t:" << item.name << '='
        << (item.expression ? ExpressionIdentity(item.expression)
                            : std::string());
  }
  if (operation == LogicalOperator::kSort ||
      operation == LogicalOperator::kTopN) {
    out << "#s:";
    for (size_t i = 0; i < target_list.size(); ++i) {
      out << (i < sort_ascending.size() && sort_ascending[i] ? 'a' : 'd');
      if (i < sort_nulls_first.size()) {
        out << (sort_nulls_first[i].value_or(false) ? 'f' : 'l');
      } else {
        out << 'd';
      }
    }
  }
  if (operation == LogicalOperator::kLimit) {
    out << "#l:" << limit_offset << ',' << limit_count;
  }
  if (operation == LogicalOperator::kTopN) {
    out << "#l:" << limit_offset << ',' << limit_count;
    if (with_ties) {
      out << ",ties";
    }
  }
  if (operation == LogicalOperator::kRelational) {
    out << "#relational:"
        << static_cast<const void*>(relational_statement.get());
  }
  if (operation == LogicalOperator::kValues) {
    out << "#v:" << output_schema.ColumnCount() << ':' << values.size();
    for (const Row& row : values) {
      out << ':' << row;
    }
  }
  if (output_schema.ColumnCount() > 0) {
    out << "#sc:";
    for (size_t i = 0; i < output_schema.ColumnCount(); ++i) {
      const auto& col = output_schema.GetColumn(i);
      out << col.Name().ToString() << ':' << static_cast<int>(col.Type()) << ':'
          << static_cast<int>(col.GetConstraint().ctype) << ';';
    }
  }
  if (!partition_by.empty()) {
    out << "#pb:";
    for (const Expression& expr : partition_by) {
      out << (expr ? ExpressionIdentity(expr) : std::string()) << ';';
    }
  }
  if (!grouping_sets.empty()) {
    out << "#gs:";
    for (const Expression& expr : grouping_sets) {
      out << (expr ? ExpressionIdentity(expr) : std::string()) << ';';
    }
  }
  if (!unnest_alias.empty()) {
    out << "#u:" << unnest_alias;
  }
  if (!offset_alias.empty()) {
    out << "#uo:" << offset_alias;
  }
  if (!cte_name.empty()) {
    out << "#cte:" << cte_name << ':' << depth_limit;
  }
  if (depth_spec.has_value()) {
    out << "#ds:" << depth_spec->column << ':' << depth_spec->lower << ':'
        << depth_spec->upper;
  }
  if (operation == LogicalOperator::kApply) {
    out << "#apply:" << static_cast<const void*>(relational_statement.get())
        << ':' << static_cast<int>(join_type);
  }
  if (depth_spec.has_value()) {
    out << "#ds:" << depth_spec->column << ':' << depth_spec->lower << ':'
        << depth_spec->upper;
  }
  if (operation == LogicalOperator::kApply) {
    out << "#apply:" << static_cast<const void*>(relational_statement.get())
        << ':' << static_cast<int>(join_type);
  }
  if (operation == LogicalOperator::kSample) {
    out << "#sample:" << TableSampleMethodName(sample_method) << ':'
        << TableSampleUnitName(sample_unit) << ':';
    if (sample_size) {
      out << ExpressionIdentity(sample_size);
    }
    out << ':';
    if (sample_seed && *sample_seed) {
      out << ExpressionIdentity(*sample_seed);
    }
    out << ':' << sample_with_weight << ':' << sample_weight_alias;
    for (const Expression& partition : partition_by) {
      out << ":p=";
      if (partition) {
        out << ExpressionIdentity(partition);
      }
    }
  }
  return out.str();
}

std::string Memo::GroupKey(const std::vector<std::string>& relations) {
  std::string key;
  for (const std::string& relation : relations) {
    key.append(std::to_string(relation.size()));
    key.push_back(':');
    key.append(relation);
    key.push_back(';');
  }
  return key;
}

StatusOr<GroupId> Memo::TryBuild(const std::vector<std::string>& relations) {
  return TryBuild(relations, {});
}

StatusOr<GroupId> Memo::TryBuild(const std::vector<std::string>& relations,
                                 const std::vector<ConjunctInfo>& conjuncts) {
  if (relations.empty()) {
    return StatusError(StatusCode::kInvalidArgument, "empty join graph");
  }
  if (Normalize(relations).size() != relations.size()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "duplicate relation in join graph");
  }
  if (relations.size() >= std::numeric_limits<uint64_t>::digits) {
    return StatusError(StatusCode::kInvalidArgument,
                       "join graph is too large for enumeration");
  }
  relation_index_.clear();
  const std::vector<std::string> normalized = Normalize(relations);
  for (size_t i = 0; i < normalized.size(); ++i) {
    relation_index_.emplace(normalized[i], i);
  }
  conjuncts_.clear();
  conjunct_masks_.clear();
  for (const ConjunctInfo& info : conjuncts) {
    uint64_t mask = 0;
    bool outside = false;
    for (const std::string& relation : info.relations) {
      const auto found = relation_index_.find(relation);
      if (found == relation_index_.end()) {
        outside = true;
        continue;
      }
      mask |= uint64_t{1} << found->second;
    }
    // A conjunct that references something outside the join graph stays at
    // the root join (it can never be pushed below).
    conjunct_masks_.push_back(outside ? ~uint64_t{0} : mask);
    conjuncts_.push_back(info.conjunct);
  }
  return EnsureGroup(relations);
}

uint64_t Memo::RelationMask(const std::vector<std::string>& relations) const {
  uint64_t mask = 0;
  for (const std::string& relation : relations) {
    const auto found = relation_index_.find(relation);
    if (found == relation_index_.end()) {
      CHECK_MSG(false, "relation outside the join graph: " + relation);
    }
    mask |= uint64_t{1} << found->second;
  }
  return mask;
}

bool Memo::ContainsRelation(const std::vector<std::string>& relations) const {
  return std::ranges::all_of(relations, [this](const std::string& relation) {
    return relation_index_.contains(relation);
  });
}

Expression Memo::ScanFilterFor(const Group& group) const {
  std::vector<Expression> matching;
  for (size_t i = 0; i < conjuncts_.size(); ++i) {
    if (conjunct_masks_[i] == group.relation_mask) {
      matching.push_back(conjuncts_[i]);
    }
  }
  if (matching.empty()) {
    return nullptr;
  }
  return CanonicalizeConjuncts(CombineConjuncts(matching));
}

Expression Memo::JoinConditionFor(const Group& left, const Group& right) const {
  const uint64_t union_mask = left.relation_mask | right.relation_mask;
  std::vector<Expression> spanning;
  for (size_t i = 0; i < conjuncts_.size(); ++i) {
    const uint64_t mask = conjunct_masks_[i];
    if ((mask & ~union_mask) != 0) {
      continue;
    }
    if (mask == 0) {
      continue;
    }
    if ((mask & ~left.relation_mask) == 0) {
      continue;
    }
    if ((mask & ~right.relation_mask) == 0) {
      continue;
    }
    spanning.push_back(conjuncts_[i]);
  }
  if (spanning.empty()) {
    return nullptr;
  }
  return CanonicalizeConjuncts(CombineConjuncts(spanning));
}

// Chooses the first relation whose singleton cut is crossed by a conjunct so
// the initial join tree avoids cross products whenever the subgraph is
// connected (Phase 7 pruning also applies to memo construction).
namespace {
std::pair<std::vector<std::string>, std::vector<std::string>> ConnectedSplit(
    const Memo& memo, const std::vector<std::string>& relations) {
  const uint64_t within = memo.RelationMask(relations);
  for (const std::string& pivot : relations) {
    const uint64_t pivot_mask = memo.RelationMask({pivot});
    if (!memo.CutConnected(pivot_mask, within)) {
      continue;
    }
    std::vector<std::string> left{pivot};
    std::vector<std::string> right;
    for (const std::string& relation : relations) {
      if (relation != pivot) {
        right.push_back(relation);
      }
    }
    return {std::move(left), std::move(right)};
  }
  std::vector<std::string> left{relations.front()};
  std::vector<std::string> right(relations.begin() + 1, relations.end());
  return {std::move(left), std::move(right)};
}
}  // namespace

LogicalExpression Memo::NewJoin(GroupId left, GroupId right,
                                const Expression& inherited_condition) const {
  LogicalExpression join{.operation = LogicalOperator::kJoin,
                         .children = {left, right}};
  const Expression condition = inherited_condition
                                   ? CanonicalizeConjuncts(inherited_condition)
                                   : JoinConditionFor(Get(left), Get(right));
  if (condition) {
    join.predicate = condition;
  }
  return join;
}

LogicalExpression Memo::NewOuterJoin(GroupId left, GroupId right,
                                     Expression on_condition,
                                     uint8_t join_type) {
  LogicalExpression join{.operation = LogicalOperator::kOuterJoin,
                         .children = {left, right}};
  if (on_condition) {
    join.predicate = std::move(on_condition);
  }
  join.join_type = join_type;
  return join;
}

void Memo::MergeScanFilter(GroupId group, const Expression& predicate) {
  Group& target = Get(group);
  if (target.sample_barrier) {
    return;
  }
  if (target.relations.size() != 1) {
    CHECK_MSG(false, "scan filter requires a single-relation group");
  }
  const Expression next = CanonicalizeConjuncts(
      target.filter
          ? BinaryExpressionExp(target.filter, BinaryOperation::kAnd, predicate)
          : predicate);
  if ((!target.filter && !next) ||
      (target.filter && next &&
       target.filter->ToString() == next->ToString())) {
    return;
  }
  target.filter = next;
  ++revision_;
  touched_groups_.push_back(group);
}

bool Memo::CutConnected(uint64_t left_mask, uint64_t within_mask) const {
  const uint64_t right_mask = within_mask & ~left_mask;
  if (left_mask == 0 || right_mask == 0) {
    return false;
  }
  return std::ranges::any_of(
      conjunct_masks_, [left_mask, right_mask](uint64_t mask) {
        return (mask & left_mask) != 0 && (mask & right_mask) != 0;
      });
}

bool Memo::JoinGraphDisconnected() const {
  return std::ranges::none_of(conjunct_masks_, [](uint64_t mask) {
    return mask != 0 && std::popcount(mask) > 1;
  });
}

bool Memo::JoinGraphConnected() const {
  uint64_t all_relations = 0;
  for (const auto& [relation, index] : relation_index_) {
    (void)relation;
    all_relations |= uint64_t{1} << index;
  }
  return all_relations != 0 && InducedSubgraphConnected(all_relations);
}

bool Memo::HasColumnEqualityConjunct() const {
  for (size_t i = 0; i < conjuncts_.size(); ++i) {
    if (i >= conjunct_masks_.size() || std::popcount(conjunct_masks_[i]) < 2 ||
        !conjuncts_[i] || conjuncts_[i]->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjuncts_[i]->AsBinaryExpression();
    if ((binary.Op() == BinaryOperation::kEquals ||
         binary.Op() == BinaryOperation::kIsNotDistinctFrom) &&
        binary.Left()->Type() == TypeTag::kColumnValue &&
        binary.Right()->Type() == TypeTag::kColumnValue) {
      return true;
    }
  }
  return false;
}

uint64_t Memo::NeighborBits(uint64_t subset, uint64_t within) const {
  if (subset == 0 || within == 0) {
    return 0;
  }
  uint64_t neighbors = 0;
  for (const uint64_t mask : conjunct_masks_) {
    if (std::popcount(mask) < 2 || (mask & subset) == 0) {
      continue;
    }
    neighbors |= mask & within & ~subset;
  }
  return neighbors;
}

bool Memo::InducedSubgraphConnected(uint64_t within) const {
  if (within == 0) {
    return false;
  }
  if (std::popcount(within) <= 1) {
    return true;
  }
  const uint64_t start = within & -within;
  uint64_t seen = start;
  std::vector<uint64_t> stack{start};
  while (!stack.empty()) {
    const uint64_t node = stack.back();
    stack.pop_back();
    uint64_t pending = NeighborBits(node, within) & ~seen;
    while (pending != 0) {
      const uint64_t bit = pending & -pending;
      pending ^= bit;
      seen |= bit;
      stack.push_back(bit);
    }
  }
  return seen == within;
}

bool Memo::ClaimJoinEnumeration(GroupId group) {
  if (join_enumerated_.size() < groups_.size()) {
    join_enumerated_.resize(groups_.size());
  }
  if (group >= join_enumerated_.size() || join_enumerated_[group] != 0) {
    return false;
  }
  join_enumerated_[group] = 1;
  return true;
}

std::vector<GroupId> Memo::DrainTouchedGroups() {
  std::vector<GroupId> touched;
  std::swap(touched, touched_groups_);
  return touched;
}

GroupId Memo::EnsureGroup(
    std::vector<std::string>
        relations) {  // NOLINT(misc-no-recursion) // Cascades memo construction
                      // recurses over join operands by design; relation count
                      // is bounded (kMaxJoinEnumerationRelations).
  relations = Normalize(std::move(relations));
  if (relations.empty()) {
    CHECK_MSG(false, "empty memo group");
  }
  const std::string key = GroupKey(relations);
  if (const auto found = groups_by_key_.find(key);
      found != groups_by_key_.end()) {
    return found->second;
  }

  const GroupId id = groups_.size();
  groups_by_key_.emplace(key, id);
  const uint64_t mask = RelationMask(relations);
  groups_.push_back(Group{.id = id,
                          .relations = relations,
                          .expressions = {},
                          .filter = nullptr,
                          .relation_mask = mask,
                          .tag = "",
                          .logical_properties = {}});
  if (relations.size() == 1) {
    groups_.back().filter = ScanFilterFor(groups_.back());
    AddExpression(id, LogicalExpression{.operation = LogicalOperator::kScan,
                                        .table = relations.front()});
    return id;
  }

  auto [left, right] = relations.size() > 16
                           ? GreedyConnectedSplit(*this, relations)
                           : ConnectedSplit(*this, relations);
  const GroupId left_group = EnsureGroup(std::move(left));
  const GroupId right_group = EnsureGroup(std::move(right));
  AddExpression(id, NewJoin(left_group, right_group));
  return id;
}

GroupId Memo::EnsureDerivedGroup(const std::vector<std::string>& relations,
                                 std::string_view tag) {
  const std::vector<std::string> normalized = Normalize(relations);
  std::string key(tag);
  key.push_back('|');
  key.append(GroupKey(normalized));
  if (const auto found = groups_by_key_.find(key);
      found != groups_by_key_.end()) {
    return found->second;
  }
  const GroupId id = groups_.size();
  groups_by_key_.emplace(std::move(key), id);
  groups_.push_back(Group{.id = id,
                          .relations = normalized,
                          .expressions = {},
                          .filter = nullptr,
                          .relation_mask = RelationMask(normalized),
                          .tag = std::string(tag),
                          .logical_properties = {}});
  return id;
}

bool Memo::AddExpression(GroupId group, LogicalExpression expression) {
  CHECK_MSG(group < groups_.size(), "memo group");
  Group& target = groups_[group];
  // Normalize an engaged-but-null predicate (Expression{} is a null
  // shared_ptr) to nullopt: callers only test has_value() / operator bool,
  // so an optional holding a null handle dereferences as null downstream
  // (SIGSEGV in Fingerprint and rule bodies).
  if (expression.predicate.has_value() && !*expression.predicate) {
    expression.predicate.reset();
  }
  // Physical aggregation accepts the legacy partition_by spelling as a
  // grouping-key list. Canonicalize it before properties, fingerprints and
  // logical rewrites inspect the node; otherwise they mistake it for a scalar
  // aggregate (and may manufacture an invalid empty projection).
  if (expression.operation == LogicalOperator::kAggregation) {
    if (expression.grouping_sets.empty()) {
      expression.grouping_sets = std::move(expression.partition_by);
    }
    expression.partition_by.clear();
  }
  switch (expression.operation) {
    case LogicalOperator::kScan:
      if (expression.table.empty() || !expression.children.empty() ||
          expression.predicate ||
          (target.relations.size() == 1
               ? (target.relations.front() != expression.table &&
                  !IsSameTable(target.relations.front(), expression.table))
               : !std::ranges::all_of(
                     target.relations, [&](const std::string& rel) {
                       return IsSameTable(rel, expression.table);
                     }))) {
        CHECK_MSG(false, "scan does not belong to memo group");
      }
      break;
    case LogicalOperator::kValues:
    case LogicalOperator::kDummyScan:
    case LogicalOperator::kRelational:
      break;
    case LogicalOperator::kJoin:
    case LogicalOperator::kOuterJoin:
    case LogicalOperator::kCrossJoin:
    case LogicalOperator::kApply: {
      if (expression.children.size() == 1) {
        if (expression.children[0] == target.id) {
          CHECK_MSG(false, "logical operator references its own group");
        }
        break;
      }
      if (expression.children.size() != 2) {
        CHECK_MSG(false, "apply must have one or two child groups");
      }
      const Group& left = Get(expression.children[0]);
      const Group& right = Get(expression.children[1]);
      std::vector<std::string> intersection;
      std::ranges::set_intersection(left.relations, right.relations,
                                    std::back_inserter(intersection));
      if (!intersection.empty() ||
          UnionRelations(left.relations, right.relations) != target.relations) {
        CHECK_MSG(false, "apply children are not equivalent to group");
      }
      break;
    }
    case LogicalOperator::kRecursiveCte: {
      if (expression.children.empty() || expression.children.size() == 1) {
        break;
      }
      if (expression.children.size() != 2) {
        CHECK_MSG(false, "recursive cte must have at most two child groups");
      }
      const Group& left = Get(expression.children[0]);
      const Group& right = Get(expression.children[1]);
      std::vector<std::string> intersection;
      std::ranges::set_intersection(left.relations, right.relations,
                                    std::back_inserter(intersection));
      if (!intersection.empty() ||
          UnionRelations(left.relations, right.relations) != target.relations) {
        if (target.tag.empty()) {
          CHECK_MSG(false, "join children are not equivalent to group");
        }
      }
      break;
    }
    case LogicalOperator::kSemiJoin:
    case LogicalOperator::kAntiJoin: {
      if (expression.children.size() != 2) {
        CHECK_MSG(false, "semi/anti join must have two child groups");
      }
      const Group& left = Get(expression.children[0]);
      const Group& right = Get(expression.children[1]);
      if (left.relations != target.relations &&
          UnionRelations(left.relations, right.relations) != target.relations) {
        CHECK_MSG(false, "join children are not equivalent to group");
      }
      break;
    }
    case LogicalOperator::kUnion:
    case LogicalOperator::kUnionAll:
    case LogicalOperator::kIntersect:
    case LogicalOperator::kIntersectAll:
    case LogicalOperator::kExcept:
    case LogicalOperator::kExceptAll: {
      if (expression.children.size() < 2) {
        CHECK_MSG(false, "set operation needs at least two children");
      }
      std::vector<std::string> relations;
      for (const GroupId child : expression.children) {
        relations = UnionRelations(relations, Get(child).relations);
      }
      if (relations != target.relations) {
        CHECK_MSG(false, "set operation children are not equivalent to group");
      }
      break;
    }
    case LogicalOperator::kSelection:
    case LogicalOperator::kProjection:
    case LogicalOperator::kAggregation:
    case LogicalOperator::kSort:
    case LogicalOperator::kTopN:
    case LogicalOperator::kDistinct:
    case LogicalOperator::kLimit:
    case LogicalOperator::kEmpty:
    case LogicalOperator::kWindow:
    case LogicalOperator::kUnnest:
    case LogicalOperator::kSample: {
      if (expression.children.size() != 1) {
        CHECK_MSG(false, "single-child logical operator");
      }
      if (expression.children[0] == group) {
        CHECK_MSG(false, "logical operator references its own group");
      }
      const auto& child_rels = Get(expression.children[0]).relations;
      if (child_rels != target.relations) {
        // Self-join style operators may reference the same real table under
        // different aliases, so only enforce the relation-set invariant when
        // the sets name genuinely different tables (from fix_rules).
        bool all_same_table = !target.relations.empty() && !child_rels.empty();
        for (const auto& r : target.relations) {
          if (!IsSameTable(r, target.relations.front())) {
            all_same_table = false;
            break;
          }
        }
        for (const auto& r : child_rels) {
          if (!IsSameTable(r, target.relations.front())) {
            all_same_table = false;
            break;
          }
        }
        if (!all_same_table) {
          CHECK_MSG(false, "operator must preserve the group's relation set");
        }
      }
      if (expression.operation == LogicalOperator::kSelection &&
          (!expression.predicate || !*expression.predicate)) {
        CHECK_MSG(false, "selection must carry a predicate");
      }
      if ((expression.operation == LogicalOperator::kProjection ||
           expression.operation == LogicalOperator::kAggregation) &&
          expression.target_list.empty()) {
        CHECK_MSG(false, "projection/aggregation needs a target list");
      }
      if ((expression.operation == LogicalOperator::kSort ||
           expression.operation == LogicalOperator::kTopN) &&
          (expression.target_list.empty() ||
           expression.target_list.size() != expression.sort_ascending.size())) {
        CHECK_MSG(false, "sort needs one direction per key");
      }
      if (expression.operation == LogicalOperator::kTopN &&
          expression.limit_count == 0) {
        CHECK_MSG(false, "top-n needs a finite limit");
      }
      break;
    }
  }
  // Duplicate rejection runs BEFORE the cap check counts against it, but a
  // rejected duplicate must not trip the degradation flag either (§6.10):
  // mirrored associativity rotations re-derive existing expressions by
  // design and those retries have to stay free.
  // The fingerprint starts with the operator, table and child groups. Most
  // join alternatives differ there already; avoid serializing their complete
  // predicate trees merely to reject an unequal prefix. Compute a candidate
  // identity lazily, and keep recomputing existing payloads when needed so
  // externally rebound parameters or qualified columns cannot stale a cache.
  std::optional<std::string> fingerprint;
  const bool duplicate =
      std::ranges::any_of(target.expressions, [&](const auto& existing) {
        if (existing.operation != expression.operation ||
            existing.table != expression.table ||
            existing.children != expression.children) {
          return false;
        }
        if (!fingerprint) {
          fingerprint = expression.Fingerprint();
        }
        return existing.Fingerprint() == *fingerprint;
      });
  if (!duplicate && target.expressions.size() >= expression_cap_) {
    degraded_ = true;
  }
  if (duplicate || target.expressions.size() >= expression_cap_) {
    return false;
  }
  if (target.logical_properties_expression_count != target.expressions.size()) {
    target.logical_properties_expression_count.reset();
    target.logical_properties_stale = true;
  }
  for (const GroupId child : expression.children) {
    if (child >= groups_.size() || child == group) {
      continue;
    }
    auto& dependents = groups_[child].logical_property_dependents;
    if (std::ranges::find(dependents, group) == dependents.end()) {
      dependents.push_back(group);
    }
  }
  target.expressions.PushBack(std::move(expression));
  ++revision_;
  touched_groups_.push_back(group);
  DeriveLogicalProperties(group);
  return true;
}

const Group& Memo::Get(GroupId group) const {
  CHECK_MSG(group < groups_.size(), "memo group");
  return groups_[group];
}

Group& Memo::Get(GroupId group) {
  CHECK_MSG(group < groups_.size(), "memo group");
  return groups_[group];
}

size_t Memo::ExpressionCount(GroupId group) const {
  return Get(group).expressions.size();
}

void Memo::Dump(std::ostream& out) const {
  for (const Group& group : groups_) {
    out << "group " << group.id << " relations={";
    for (size_t i = 0; i < group.relations.size(); ++i) {
      if (i > 0) {
        out << ",";
      }
      out << group.relations[i];
    }
    out << "}";
    if (!group.tag.empty()) {
      out << " tag=" << group.tag;
    }
    if (group.filter) {
      out << " filter=" << *group.filter;
    }
    out << " expressions=" << group.expressions.size() << "\n";
    for (const LogicalExpression& expression : group.expressions) {
      out << "  [" << expression.Fingerprint() << "]\n";
    }
  }
}

bool IsStrictOnRelations(const Expression& expr,
                         const std::vector<std::string>& relations) {
  return IsNullStrictWhere(
      expr,
      [&](const ColumnName& col) {
        return std::ranges::any_of(relations, [&](const std::string& rel) {
          return rel == col.schema;
        });
      },
      /*in_exp_is_strict=*/false);
}

bool ExpressionRejectsNullsOnRelations(
    const Expression& expr, const std::vector<std::string>& relations) {
  return ExpressionRejectsNullsWhere(
      expr,
      [&](const ColumnName& col) {
        return std::ranges::any_of(relations, [&](const std::string& rel) {
          return rel == col.schema;
        });
      },
      /*bare_column_rejects_null=*/false);
}

bool IsStrictOnColumn(const Expression& expr, const std::string& column_name) {
  return IsNullStrictWhere(
      expr,
      [&](const ColumnName& col) {
        return col.name == column_name || col.ToString() == column_name;
      },
      /*in_exp_is_strict=*/false);
}

bool ExpressionRejectsNullsOnColumn(const Expression& expr,
                                    const std::string& column_name) {
  return ExpressionRejectsNullsWhere(
      expr,
      [&](const ColumnName& col) {
        return col.name == column_name || col.ToString() == column_name;
      },
      /*bare_column_rejects_null=*/false);
}

namespace {

bool HasCanonicalStoredJoinPredicate(const Memo& memo, GroupId left,
                                     GroupId right,
                                     const LogicalExpression& expression) {
  const Expression stored =
      memo.JoinConditionFor(memo.Get(left), memo.Get(right));
  if (!expression.predicate || !*expression.predicate) {
    return !stored;
  }
  if (!stored) {
    return false;
  }
  return CanonicalizeConjuncts(*expression.predicate)->ToString() ==
         stored->ToString();
}

// Partitions `group` into join pairs. Connected graphs use DPHyp's csg-cmp
// restriction so a chain or star does not materialize cross-product splits.
void EnumerateJoinGroup(Memo& memo, GroupId group,
                        const LogicalExpression& expression) {
  const std::vector<std::string> relations = memo.Get(group).relations;
  if (relations.size() < 3 || relations.size() > kMaxJoinEnumerationRelations) {
    return;
  }
  if (!HasCanonicalStoredJoinPredicate(memo, expression.children[0],
                                       expression.children[1], expression)) {
    return;
  }
  if (!memo.ClaimJoinEnumeration(group)) {
    return;
  }
  const uint64_t within = memo.Get(group).relation_mask;
  const bool connected = memo.InducedSubgraphConnected(within);
  const bool prune_open_cuts = !memo.JoinGraphDisconnected();
  const uint64_t limit = uint64_t{1} << relations.size();
  std::vector<uint64_t> global_bit(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    global_bit[i] = memo.RelationMask({relations[i]});
  }
  for (uint64_t local = 1; local + 1 < limit; ++local) {
    if ((local & 1U) == 0) {
      continue;
    }
    uint64_t left_global = 0;
    std::vector<std::string> left;
    std::vector<std::string> right;
    left.reserve(relations.size());
    right.reserve(relations.size());
    for (size_t i = 0; i < relations.size(); ++i) {
      if (((local >> i) & 1U) != 0) {
        left_global |= global_bit[i];
        left.push_back(relations[i]);
      } else {
        right.push_back(relations[i]);
      }
    }
    if (prune_open_cuts && !memo.CutConnected(left_global, within)) {
      continue;
    }
    if (connected) {
      const uint64_t right_global = within & ~left_global;
      if (!memo.InducedSubgraphConnected(left_global) ||
          !memo.InducedSubgraphConnected(right_global)) {
        continue;
      }
    }
    const GroupId left_group = memo.EnsureGroup(std::move(left));
    const GroupId right_group = memo.EnsureGroup(std::move(right));
    memo.AddExpression(group, memo.NewJoin(left_group, right_group));
  }
}

// The safe exception for Limit under Window (todo: window下のLIMITの
// 安全な例外).  A window emits its rows in child order (the evaluator appends
// computed values per original index), so Limit(k) over a window keeps
// exactly the first k child rows.  Moving the limit below the window keeps
// those same rows but computes their values over truncated partitions; the
// values survive that when every value-determining row of a survivor sits at
// or before it in partition order (backward-only frames, prefix-counting
// ranking calls) AND truncation can only remove later rows (the child emits
// rows in partition order).  The helpers below encode each half.

// Length-prefixed ordering signature of one window call (raw separators
// collide when a key's own text contains them, mirroring the window-layer
// grouping in optimizer.cpp).
std::string WindowSpecSignature(const WindowFunctionCallExpression& call) {
  std::string signature = std::to_string(call.partition_by.size()) + ":";
  for (const Expression& key : call.partition_by) {
    const std::string text = key ? key->ToString() : std::string("<null>");
    signature += std::to_string(text.size()) + "/" + text + ";";
  }
  signature += std::to_string(call.order_by.size()) + ":";
  for (const WindowOrderTerm& term : call.order_by) {
    const std::string text =
        term.expression ? term.expression->ToString() : std::string("<null>");
    signature += std::to_string(text.size()) + "/" + text + "/a";
    signature += term.ascending ? "1" : "0";
    signature += "/n";
    signature += !term.nulls_first.has_value()
                     ? "d"
                     : (term.nulls_first.value() ? "1" : "0");
    signature += ";";
  }
  return signature;
}

// Does this call keep producing the same value for a row after some of its
// partition's later rows were truncated away?
bool WindowValueStableUnderTruncation(
    const WindowFunctionCallExpression& call) {
  // Ranking values are prefix counts in partition order: ROW_NUMBER numbers
  // the rows 1..m, RANK counts strictly smaller keys (+1), DENSE_RANK counts
  // key groups.  Removing a row strictly after a survivor in partition order
  // cannot change any of them for rows the truncation keeps.
  if (call.function == "ROW_NUMBER" || call.function == "RANK" ||
      call.function == "DENSE_RANK") {
    return true;
  }
  // These functions ignore ROWS bounds. Even an explicitly backward frame
  // cannot make their partition-size / positional result prefix-stable.
  if (call.function == "NTILE" || call.function == "PERCENT_RANK" ||
      call.function == "CUME_DIST" || call.function == "LEAD" ||
      call.function == "LAG" || call.function == "PERCENTILE_CONT" ||
      call.function == "PERCENTILE_DISC") {
    return false;
  }
  // Every other call needs an explicit ROWS frame that starts and ends at or
  // before the current row: both bounds then read rows the survivor keeps.
  // The default frame (RANGE ... CURRENT ROW, peer-inclusive, or the whole
  // partition when there are no order keys), RANGE/GROUPS frames, calls that
  // need the partition size (NTILE, PERCENT_RANK, CUME_DIST) and forward
  // peering calls (LEAD) change value under truncation and stay excluded.
  if (!call.has_frame || call.frame_unit != WindowFrameUnit::kRows) {
    return false;
  }
  const auto reads_backward = [](WindowFrameBoundType type) {
    return type == WindowFrameBoundType::kUnboundedPreceding ||
           type == WindowFrameBoundType::kOffsetPreceding ||
           type == WindowFrameBoundType::kCurrentRow;
  };
  return reads_backward(call.frame_start.type) &&
         reads_backward(call.frame_end.type);
}

// Gate for the two limit-prefix window rules.  Returns nullopt when the
// window node cannot take the transform at all (non-call targets or calls
// with mixed specs); otherwise the shared spec, the shared order-term count
// (one call's count decides all of them) and whether all calls keep their
// values under truncation.
struct WindowPrefixGateResult {
  std::string spec;
  size_t order_terms{0};
  bool stable{true};
};
std::optional<WindowPrefixGateResult> WindowPrefixGate(
    const LogicalExpression& window) {
  if (window.children.size() != 1 || window.target_list.empty()) {
    return std::nullopt;
  }
  WindowPrefixGateResult gate;
  for (const NamedExpression& target : window.target_list) {
    if (!target.expression ||
        target.expression->Type() != TypeTag::kWindowFunctionExp) {
      return std::nullopt;
    }
    const auto& call = target.expression->AsWindowFunctionCallExpression();
    const std::string spec = WindowSpecSignature(call);
    if (gate.spec.empty()) {
      gate.spec = spec;
      gate.order_terms = call.order_by.size();
    } else if (gate.spec != spec) {
      return std::nullopt;
    }
    if (!WindowValueStableUnderTruncation(call)) {
      gate.stable = false;
    }
  }
  return gate;
}

// Any window reachable below the window's child group: the transform only
// cuts rows between the bound window and its child, so a deeper window
// feeding this one would see a truncated input without having the same
// proof made for it — refuse (the gate can learn to recurse later).
bool ReachableChildHasWindow(const Memo& memo, GroupId start) {
  std::vector<char> seen(memo.GroupCount(), 0);
  std::vector<GroupId> pending{start};
  while (!pending.empty()) {
    const GroupId next = pending.back();
    pending.pop_back();
    if (static_cast<size_t>(next) >= seen.size() || (seen[next] != 0)) {
      continue;
    }
    seen[next] = 1;
    for (const LogicalExpression& expression : memo.Get(next).expressions) {
      if (expression.operation == LogicalOperator::kWindow) {
        return true;
      }
      pending.insert(pending.end(), expression.children.begin(),
                     expression.children.end());
    }
  }
  return false;
}

// Does a sort node implement exactly the window spec's emitted order (the
// partition keys as a contiguous prefix — grouping only needs contiguity,
// so their sort flags are free — followed by the order terms with their
// exact ascending and NULLS placement)?
bool SortDeliversWindowSpec(const LogicalExpression& sort,
                            const WindowFunctionCallExpression& call) {
  if (sort.target_list.size() != sort.sort_ascending.size() ||
      sort.target_list.size() !=
          call.partition_by.size() + call.order_by.size()) {
    return false;
  }
  for (size_t i = 0; i < call.partition_by.size(); ++i) {
    const Expression& part = call.partition_by[i];
    const std::string text = part ? part->ToString() : std::string("<null>");
    if (!sort.target_list[i].expression ||
        sort.target_list[i].expression->ToString() != text) {
      return false;
    }
  }
  for (size_t j = 0; j < call.order_by.size(); ++j) {
    const WindowOrderTerm& term = call.order_by[j];
    const size_t index = call.partition_by.size() + j;
    if (!term.expression || !sort.target_list[index].expression ||
        term.expression->ToString() !=
            sort.target_list[index].expression->ToString() ||
        term.ascending != sort.sort_ascending[index]) {
      return false;
    }
    // Engine default NULLS placement: NULLS FIRST on ascending, LAST on
    // descending; SortExecutor resolves an absent flag with the same rule
    // (sort.cpp: nulls_first = value_or(ascending)), so compare the
    // resolved values.
    const std::optional<bool> null_flag = index < sort.sort_nulls_first.size()
                                              ? sort.sort_nulls_first[index]
                                              : std::nullopt;
    if (term.nulls_first.value_or(term.ascending) !=
        null_flag.value_or(sort.sort_ascending[index])) {
      return false;
    }
  }
  return true;
}

}  // namespace

const RuleSet& RuleSet::Default() {
  static const RuleSet rules = [] {
    using namespace dsl;
    RuleSet built;
    built.Add(Rule(
        "join_enumeration", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          EnumerateJoinGroup(memo, group, expression);
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "join_commutativity", Join(Any("left"), Any("right")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (memo.JoinEnumerationClaimed(group)) {
            return;
          }
          memo.AddExpression(
              group,
              memo.NewJoin(
                  expression.children[1], expression.children[0],
                  expression.predicate ? *expression.predicate : Expression{}));
        },
        LogicalOperator::kJoin));
    // Two complementary associativity rotations (§6.10). They look redundant,
    // but the worklist applies each rule once per expression occurrence, so
    // neither direction alone sees every child-group state in time; together
    // they derive every join shape. Their overlapping OUTPUTS are duplicates
    // by fingerprint, and Memo::AddExpression rejects those for free (before
    // the cap), so the pair no longer pollutes the expression budget.
    built.Add(Rule(
        "join_associativity_left",
        Join(
            Pattern::Op(LogicalOperator::kJoin, {Any("ll"), Any("lr")}, "left"),
            Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (memo.JoinEnumerationClaimed(group) ||
              !HasCanonicalStoredJoinPredicate(memo, expression.children[0],
                                               expression.children[1],
                                               expression)) {
            return;
          }
          const GroupId inner = memo.EnsureGroup(
              UnionRelations(memo.Get(bindings.at("lr")).relations,
                             memo.Get(bindings.at("right")).relations));
          memo.AddExpression(group, memo.NewJoin(bindings.at("ll"), inner));
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "join_associativity_right",
        Join(Any("left"), Pattern::Op(LogicalOperator::kJoin,
                                      {Any("rl"), Any("rr")}, "right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (memo.JoinEnumerationClaimed(group) ||
              !HasCanonicalStoredJoinPredicate(memo, expression.children[0],
                                               expression.children[1],
                                               expression)) {
            return;
          }
          const GroupId inner = memo.EnsureGroup(
              UnionRelations(memo.Get(bindings.at("left")).relations,
                             memo.Get(bindings.at("rl")).relations));
          memo.AddExpression(group, memo.NewJoin(inner, bindings.at("rr")));
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "join_to_cross_if_no_predicate", Join(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.predicate) {
            return;
          }
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kCrossJoin,
                                       .children = {bindings.at("left"),
                                                    bindings.at("right")}});
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "eliminate_false_selection", Selection(Any()),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate ||
              (*expression.predicate)->Type() != TypeTag::kConstantValue) {
            return;
          }
          const Value value =
              (*expression.predicate)->AsConstantValue().GetValue();
          if (!value.IsNull() && value.Truthy()) {
            return;
          }
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kEmpty,
                                       .children = expression.children});
        },
        LogicalOperator::kSelection));
    // Selection(Selection(X, p1), p2) -> Selection(X, p1 AND p2): adds the
    // merged expression (same child group) with canonical conjunct order.
    built.Add(Rule(
        "merge_selections", Selection(Selection(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kSelection ||
                inner.children.empty() || inner.children[0] == group) {
              continue;
            }
            const Expression merged = CanonicalizeConjuncts(
                BinaryExpressionExp(*inner.predicate, BinaryOperation::kAnd,
                                    *expression.predicate));
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = inner.children,
                                  .predicate = merged});
          }
        },
        LogicalOperator::kSelection));
    // Selection(Scan(t), p): annotate the scan group's filter so
    // implementation rules can choose IndexScan/RangeScan. Guard rail: never
    // push through outer joins (null-rejection analysis does not exist yet).
    built.Add(Rule(
        "push_selection_into_scan", SelectionWithin(0, Scan("scan")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const GroupId input = bindings.at("scan");
          if (memo.Get(input).sample_barrier ||
              memo.Get(group).sample_barrier) {
            return;
          }
          // The filtered result owns a scan alternative. Never strengthen the
          // shared input: another UNION/outer-join branch may need all its
          // rows.
          if (memo.Get(input).filter) {
            memo.MergeScanFilter(group, memo.Get(input).filter);
          }
          memo.MergeScanFilter(group, *expression.predicate);
          for (const auto& scan : memo.Get(input).expressions) {
            if (scan.operation == LogicalOperator::kScan) {
              memo.AddExpression(group, scan);
            }
          }
        },
        LogicalOperator::kSelection));
    // Selection(Join(L, R), p) with p touching only relations of L: move the
    // single-relation conjuncts of p into the scan groups of that side. The
    // Selection keeps applying whatever could not be pushed (idempotent).
    // Guard rail: no outer-join pushdown until null-rejection analysis.
    built.Add(Rule(
        "push_selection_through_join", Selection(Join(Any(), Any(), "input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!SelectionMayStrengthenSharedScans(memo, group)) {
            return;
          }
          std::unordered_set<std::string> left_relations;
          for (const LogicalExpression& join :
               memo.Get(bindings.at("input")).expressions) {
            if (join.operation != LogicalOperator::kJoin) {
              continue;
            }
            const std::vector<std::string>& relations =
                memo.Get(join.children[0]).relations;
            left_relations.insert(relations.begin(), relations.end());
          }
          PushSingleRelationConjuncts(
              memo, *expression.predicate, [&](const std::string& relation) {
                return left_relations.contains(relation);
              });
        },
        LogicalOperator::kSelection));
    // Selection(Join(L, R), A AND B) with A over L and B over R: push both
    // sides. Guard rail: no outer-join pushdown yet.
    built.Add(Rule(
        "split_selection_over_join", Selection(Join(Any(), Any(), "input")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!SelectionMayStrengthenSharedScans(memo, group)) {
            return;
          }
          PushSingleRelationConjuncts(memo, *expression.predicate,
                                      [](const std::string&) { return true; });
        },
        LogicalOperator::kSelection));
    // Selection(SetOp(children), p) is equivalent to the same set operation
    // over Selection(child, p) for every branch when p resolves in every
    // branch.  Set-operation outputs are positionally aligned, so a qualified
    // predicate such as t1.a > 0 must NOT be pushed into a branch that lacks
    // relation t1. Row-wise filtering commutes with duplicate elimination,
    // so this holds for UNION/INTERSECT/EXCEPT including ALL variants.
    built.Add(Rule(
        "push_filter_past_setop", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& setop :
               memo.Get(bindings.at("input")).expressions) {
            const bool is_setop =
                setop.operation == LogicalOperator::kUnion ||
                setop.operation == LogicalOperator::kUnionAll ||
                setop.operation == LogicalOperator::kIntersect ||
                setop.operation == LogicalOperator::kIntersectAll ||
                setop.operation == LogicalOperator::kExcept ||
                setop.operation == LogicalOperator::kExceptAll;
            if (!is_setop || !expression.predicate) {
              continue;
            }
            // Gate: every column in the predicate must resolve in every
            // branch; otherwise pushing would filter on a non-existent or
            // wrong relation.  Qualified columns resolve through the branch
            // relation lists; an unqualified column is only pushable when it
            // resolves by name in EVERY branch's catalog-published schema —
            // when the schemas are unavailable the push must refuse.
            const auto touched = (*expression.predicate)->TouchedColumns();
            const auto& schemas = memo.GetTableSchemas();
            auto resolves_in_branch = [&](const Group& branch,
                                          const ColumnName& column) {
              if (!column.schema.empty()) {
                return std::ranges::find(branch.relations, column.schema) !=
                       branch.relations.end();
              }
              if (schemas.empty()) {
                return false;
              }
              return std::ranges::all_of(
                  branch.relations, [&](const auto& rel) {
                    const auto found = schemas.find(rel);
                    return found != schemas.end() &&
                           found->second.Offset(ColumnName("", column.name)) >=
                               0;
                  });
            };
            bool pushable = true;
            for (const GroupId child : setop.children) {
              const Group& child_group = memo.Get(child);
              for (const ColumnName& column : touched) {
                if (!resolves_in_branch(child_group, column)) {
                  pushable = false;
                  break;
                }
              }
              if (!pushable) {
                break;
              }
            }
            if (!pushable) {
              continue;
            }
            std::vector<GroupId> filtered_children;
            filtered_children.reserve(setop.children.size());
            bool cycle = false;
            for (const GroupId child : setop.children) {
              const GroupId filtered = memo.EnsureDerivedGroup(
                  memo.Get(child).relations,
                  "setop-filter:" + std::to_string(child) + ":" +
                      (*expression.predicate)->ToString());
              memo.Get(filtered).branch_local = true;
              if (filtered == child || filtered == group) {
                cycle = true;
                break;
              }
              memo.AddExpression(
                  filtered,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {child},
                                    .predicate = expression.predicate});
              filtered_children.push_back(filtered);
            }
            if (cycle) {
              continue;
            }
            LogicalExpression rewritten = setop;
            rewritten.children = std::move(filtered_children);
            memo.AddExpression(group, std::move(rewritten));
          }
        },
        LogicalOperator::kSelection));
    // Selection(Distinct(R)) is equivalent to Distinct(Selection(R)): the
    // predicate depends only on row values, so duplicate elimination and
    // filtering commute without changing the distinct result.
    built.Add(Rule(
        "push_filter_through_distinct", Selection(Distinct(Any(), "input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& distinct :
               memo.Get(bindings.at("input")).expressions) {
            if (distinct.operation != LogicalOperator::kDistinct ||
                distinct.children.size() != 1 || !expression.predicate) {
              continue;
            }
            const GroupId filtered = memo.EnsureDerivedGroup(
                memo.Get(distinct.children[0]).relations,
                "filter-before-distinct:" +
                    GroupIdsTag({distinct.children[0]}) +
                    (*expression.predicate)->ToString());
            InheritBranchLocal(memo, filtered, group);
            if (filtered == group || filtered == distinct.children[0]) {
              continue;
            }
            memo.AddExpression(
                filtered,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {distinct.children[0]},
                                  .predicate = expression.predicate});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kDistinct,
                                  .children = {filtered}});
          }
        },
        LogicalOperator::kSelection));
    // Projection(UNION[X]) distributes to both branches.  Keep this rule
    // limited to UNION/UNION ALL: projection is not distributive over
    // INTERSECT/EXCEPT when the expression is non-injective.
    built.Add(Rule(
        "push_projection_through_union", Projection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& setop :
               memo.Get(bindings.at("input")).expressions) {
            if (setop.operation != LogicalOperator::kUnion &&
                setop.operation != LogicalOperator::kUnionAll) {
              continue;
            }
            std::vector<GroupId> projected_children;
            projected_children.reserve(setop.children.size());
            bool cycle = false;
            for (const GroupId child : setop.children) {
              const GroupId projected = memo.EnsureDerivedGroup(
                  memo.Get(child).relations,
                  "setop-projection:" + std::to_string(child) + ":" +
                      TargetListFingerprint(expression.target_list));
              if (projected == child || projected == group) {
                cycle = true;
                break;
              }
              memo.AddExpression(
                  projected,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {child},
                                    .target_list = expression.target_list});
              projected_children.push_back(projected);
            }
            if (cycle) {
              continue;
            }
            LogicalExpression rewritten = setop;
            rewritten.children = std::move(projected_children);
            memo.AddExpression(group, std::move(rewritten));
          }
        },
        LogicalOperator::kProjection));
    // Projection(Join(L, R)): retain only columns needed by the projection
    // and the join predicate on each side.  The top projection remains in
    // place because its expressions still define the output schema.  This is
    // deliberately limited to qualified columns and inner joins; resolving
    // ambiguous names or null-rejection for outer joins belongs to the
    // analyzer rather than to this conservative memo rewrite.
    built.Add(Rule(
        "push_projection_through_join", Projection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const Group& input_group = memo.Get(bindings.at("input"));
          // Fixpoint guard: a join this rule already built over pruned
          // children must not be pruned again (each pass would wrap another
          // projection layer around the already-projected children).
          if (input_group.tag.starts_with("join-project-join:")) {
            return;
          }
          const std::vector<std::string> input_relations =
              input_group.relations;
          for (const LogicalExpression& join : input_group.expressions) {
            if (join.operation != LogicalOperator::kJoin ||
                join.children.size() != 2) {
              continue;
            }
            if (memo.Get(join.children[0])
                    .tag.starts_with("join-project-left:") ||
                memo.Get(join.children[1])
                    .tag.starts_with("join-project-right:")) {
              continue;
            }
            const Group& left = memo.Get(join.children[0]);
            const Group& right = memo.Get(join.children[1]);
            const std::vector<std::string> left_relations = left.relations;
            const std::vector<std::string> right_relations = right.relations;
            std::vector<std::vector<ColumnName>> required(2);
            bool safe = true;
            const auto collect = [&](const Expression& item) {
              if (!item) {
                return;
              }
              for (const ColumnName& column : item->TouchedColumns()) {
                if (column.schema.empty()) {
                  safe = false;
                  return;
                }
                const bool in_left =
                    std::ranges::find(left.relations, column.schema) !=
                    left.relations.end();
                const bool in_right =
                    std::ranges::find(right.relations, column.schema) !=
                    right.relations.end();
                if (in_left == in_right) {
                  safe = false;
                  return;
                }
                auto& side = required[in_left ? 0 : 1];
                if (std::ranges::find(side, column) == side.end()) {
                  side.push_back(column);
                }
              }
            };
            for (const NamedExpression& output : expression.target_list) {
              collect(output.expression);
              if (!safe) {
                break;
              }
            }
            if (safe && join.predicate) {
              collect(*join.predicate);
            }
            if (!safe || required[0].empty() || required[1].empty()) {
              continue;
            }

            const std::string signature = [&] {
              std::string result;
              for (const auto& columns : required) {
                result.push_back('|');
                for (const ColumnName& column : columns) {
                  result.append(column.ToString());
                  result.push_back(',');
                }
              }
              return result;
            }();
            const GroupId projected_left = memo.EnsureDerivedGroup(
                left_relations,
                "join-project-left:" + GroupIdsTag({join.children[0]}) +
                    signature);
            const GroupId projected_right = memo.EnsureDerivedGroup(
                right_relations,
                "join-project-right:" + GroupIdsTag({join.children[1]}) +
                    signature);
            std::vector<NamedExpression> left_targets;
            std::vector<NamedExpression> right_targets;
            for (const ColumnName& column : required[0]) {
              left_targets.emplace_back(column);
            }
            for (const ColumnName& column : required[1]) {
              right_targets.emplace_back(column);
            }
            if (projected_left != join.children[0]) {
              memo.AddExpression(
                  projected_left,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {join.children[0]},
                                    .target_list = std::move(left_targets)});
            }
            if (projected_right != join.children[1]) {
              memo.AddExpression(
                  projected_right,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {join.children[1]},
                                    .target_list = std::move(right_targets)});
            }

            LogicalExpression rewritten_join = join;
            rewritten_join.children = {projected_left, projected_right};
            const GroupId projected_join = memo.EnsureDerivedGroup(
                input_relations,
                "join-project-join:" +
                    GroupIdsTag({join.children[0], join.children[1]}) +
                    PredicateTag(join.predicate) + signature);
            if (projected_join == group) {
              continue;
            }
            memo.AddExpression(projected_join, std::move(rewritten_join));
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = {projected_join},
                                  .target_list = expression.target_list});
          }
        },
        LogicalOperator::kProjection));
    // Projection(Projection(X)): compose the outer target list through the
    // inner one so later costing sees a single projection (Calcite
    // ProjectMerge).
    built.Add(Rule(
        "merge_projections", Projection(Projection(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& inner :
               memo.Get(bindings.at("inner")).expressions) {
            if (inner.operation != LogicalOperator::kProjection) {
              continue;
            }
            std::vector<NamedExpression> composed;
            composed.reserve(expression.target_list.size());
            bool ok = true;
            for (const NamedExpression& output : expression.target_list) {
              std::optional<Expression> rewritten =
                  RewriteThroughOutputs(output.expression, inner.target_list);
              if (!rewritten) {
                ok = false;
                break;
              }
              composed.emplace_back(output.name, *rewritten);
            }
            if (!ok || composed.empty() || inner.children.empty() ||
                inner.children[0] == group) {
              continue;
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = inner.children,
                                  .target_list = std::move(composed),
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kProjection));
    // Selection(Projection(X), p): rewrite p in terms of X and push it under
    // the projection (FilterProjectTranspose). The group keeps producing the
    // projected schema.
    built.Add(Rule(
        "push_selection_through_projection",
        Selection(Projection(Any(), "proj")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& projection :
               memo.Get(bindings.at("proj")).expressions) {
            if (projection.operation != LogicalOperator::kProjection) {
              continue;
            }
            std::optional<Expression> rewritten = RewriteThroughOutputs(
                *expression.predicate, projection.target_list);
            if (!rewritten) {
              continue;
            }
            const auto rewritten_predicate = rewritten.value_or(nullptr);
            const GroupId input = projection.children[0];
            const GroupId filtered = memo.EnsureDerivedGroup(
                memo.Get(input).relations,
                "sel-below-proj:" + GroupIdsTag({input}) +
                    rewritten_predicate->ToString());
            InheritBranchLocal(memo, filtered, group);
            if (filtered == group || filtered == input) {
              continue;
            }
            memo.AddExpression(
                filtered,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {input},
                                  .predicate = rewritten_predicate});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = {filtered},
                                  .target_list = projection.target_list});
          }
        },
        LogicalOperator::kSelection));
    // Limit(Projection(X)): project after the cut so the scan/join below
    // produces fewer rows (ProjectLimitTranspose / LimitProjectTranspose).
    built.Add(Rule(
        "push_limit_through_projection", Limit(Projection(Any(), "proj")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& projection :
               memo.Get(bindings.at("proj")).expressions) {
            if (projection.operation != LogicalOperator::kProjection) {
              continue;
            }
            const GroupId input = projection.children[0];
            const GroupId limited = memo.EnsureDerivedGroup(
                memo.Get(input).relations,
                "lim-below-proj:" + GroupIdsTag({input}) +
                    std::to_string(expression.limit_count) + ":" +
                    std::to_string(expression.limit_offset));
            if (limited == group || limited == input) {
              continue;
            }
            memo.AddExpression(
                limited,
                LogicalExpression{.operation = LogicalOperator::kLimit,
                                  .children = {input},
                                  .limit_count = expression.limit_count,
                                  .limit_offset = expression.limit_offset});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = {limited},
                                  .target_list = projection.target_list});
          }
        },
        LogicalOperator::kLimit));
    // TopN(Projection(R)) can sort R directly when every ordering expression
    // can be translated through the projection.  Keep the projection above
    // TopN so aliases and computed output columns remain unchanged.
    built.Add(Rule(
        "topn_push_through_projection", TopN(Projection(Any(), "proj")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const Group& projection_group = memo.Get(bindings.at("proj"));
          for (const LogicalExpression& projection :
               projection_group.expressions) {
            if (projection.operation != LogicalOperator::kProjection ||
                projection.children.size() != 1) {
              continue;
            }
            std::vector<NamedExpression> translated_keys;
            translated_keys.reserve(expression.target_list.size());
            bool translatable = true;
            for (const NamedExpression& key : expression.target_list) {
              const std::optional<Expression> translated =
                  RewriteThroughOutputs(key.expression, projection.target_list);
              if (!translated) {
                translatable = false;
                break;
              }
              translated_keys.emplace_back("", *translated);
            }
            if (!translatable || translated_keys.empty()) {
              continue;
            }

            const std::vector<std::string> input_relations =
                memo.Get(projection.children[0]).relations;
            std::string signature =
                "topn-below-proj:" + GroupIdsTag({projection.children[0]}) +
                std::to_string(expression.limit_count) + ":" +
                std::to_string(expression.limit_offset);
            for (const NamedExpression& key : translated_keys) {
              signature.push_back('|');
              signature.append(key.expression->ToString());
            }
            const GroupId pushed =
                memo.EnsureDerivedGroup(input_relations, signature);
            if (pushed == group || pushed == projection.children[0]) {
              continue;
            }
            memo.AddExpression(
                pushed, LogicalExpression{
                            .operation = LogicalOperator::kTopN,
                            .children = {projection.children[0]},
                            .target_list = std::move(translated_keys),
                            .sort_ascending = expression.sort_ascending,
                            .sort_nulls_first = expression.sort_nulls_first,
                            .limit_count = expression.limit_count,
                            .limit_offset = expression.limit_offset,
                            .with_ties = expression.with_ties});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = {pushed},
                                  .target_list = projection.target_list});
          }
        },
        LogicalOperator::kTopN));
    // Limit(UNION ALL(children), offset, count) only needs the first
    // offset+count rows from each child.  Keep the parent Limit so the global
    // offset remains correct, while capping every branch independently.
    built.Add(Rule(
        "union_all_push_limit", Limit(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.limit_count == 0 ||
              expression.limit_offset >
                  std::numeric_limits<size_t>::max() - expression.limit_count) {
            return;
          }
          if (memo.Get(bindings.at("input"))
                  .tag.starts_with("union-limit-setop:")) {
            return;
          }
          const size_t cap = expression.limit_offset + expression.limit_count;
          for (const LogicalExpression& setop :
               memo.Get(bindings.at("input")).expressions) {
            if (setop.operation != LogicalOperator::kUnionAll ||
                setop.children.size() < 2) {
              continue;
            }
            std::vector<GroupId> limited_children;
            limited_children.reserve(setop.children.size());
            bool cycle = false;
            for (const GroupId child : setop.children) {
              const GroupId limited = memo.EnsureDerivedGroup(
                  memo.Get(child).relations,
                  "union-limit:" + std::to_string(child) + ":" +
                      std::to_string(cap));
              if (limited == child || limited == group) {
                cycle = true;
                break;
              }
              memo.AddExpression(
                  limited,
                  LogicalExpression{.operation = LogicalOperator::kLimit,
                                    .children = {child},
                                    .limit_count = cap,
                                    .limit_offset = 0});
              limited_children.push_back(limited);
            }
            if (cycle) {
              continue;
            }
            const GroupId capped_setop = memo.EnsureDerivedGroup(
                memo.Get(bindings.at("input")).relations,
                "union-limit-setop:" + std::to_string(bindings.at("input")) +
                    ":" + std::to_string(cap));
            if (capped_setop == group) {
              continue;
            }
            LogicalExpression rewritten = setop;
            rewritten.children = std::move(limited_children);
            memo.AddExpression(capped_setop, std::move(rewritten));
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kLimit,
                                  .children = {capped_setop},
                                  .limit_count = expression.limit_count,
                                  .limit_offset = expression.limit_offset});
          }
        },
        LogicalOperator::kLimit));
    // Selection(Aggregation(X), p) for conjuncts that only mention grouping
    // keys: push them below the aggregate (FilterAggregateTranspose). Residual
    // HAVING conjuncts stay above.
    built.Add(Rule(
        "push_selection_through_aggregation",
        Selection(Aggregation(Any(), "agg")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& aggregation :
               memo.Get(bindings.at("agg")).expressions) {
            if (aggregation.operation != LogicalOperator::kAggregation) {
              continue;
            }
            const std::vector<NamedExpression> grouping =
                GroupingOutputs(aggregation.target_list);
            std::vector<Expression> pushed;
            std::vector<Expression> residual;
            for (const Expression& conjunct :
                 SplitConjuncts(*expression.predicate)) {
              std::optional<Expression> rewritten =
                  RewriteThroughOutputs(conjunct, grouping);
              if (rewritten && !ContainsAggregate(*rewritten)) {
                pushed.push_back(*rewritten);
              } else {
                residual.push_back(conjunct);
              }
            }
            if (pushed.empty()) {
              continue;
            }
            const GroupId input = aggregation.children[0];
            const GroupId filtered = memo.EnsureDerivedGroup(
                memo.Get(input).relations,
                "sel-below-agg:" + GroupIdsTag({input}) +
                    CombineConjuncts(pushed)->ToString());
            InheritBranchLocal(memo, filtered, group);
            if (filtered == group || filtered == input) {
              continue;
            }
            memo.AddExpression(
                filtered,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {input},
                                  .predicate = CombineConjuncts(pushed)});
            if (residual.empty()) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {filtered},
                                    .target_list = aggregation.target_list});
              continue;
            }
            const GroupId new_agg = memo.EnsureDerivedGroup(
                memo.Get(group).relations,
                "agg-after-sel-push:" + GroupIdsTag({filtered}) +
                    TargetListFingerprint(aggregation.target_list) + "#" +
                    CombineConjuncts(residual)->ToString());
            if (new_agg == group || new_agg == filtered) {
              continue;
            }
            memo.AddExpression(
                new_agg,
                LogicalExpression{.operation = LogicalOperator::kAggregation,
                                  .children = {filtered},
                                  .target_list = aggregation.target_list});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {new_agg},
                                  .predicate = CombineConjuncts(residual)});
          }
        },
        LogicalOperator::kSelection));
    // Inner-join equality a.x = b.y plus a.x = k implies b.y = k (predicate
    // inference / transitive closure). Outer joins are not in this memo.
    built.Add(Rule(
        "infer_join_predicates", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (SelectionMayStrengthenSharedScans(memo, group) &&
              expression.predicate) {
            InferJoinConstants(memo, *expression.predicate);
          }
        },
        LogicalOperator::kJoin));
    // Limit(Limit(X)): compose offset and take the tighter remaining count
    // (LimitMerge).
    built.Add(Rule(
        "merge_limits", Limit(Limit(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& inner :
               memo.Get(bindings.at("inner")).expressions) {
            if (inner.operation != LogicalOperator::kLimit ||
                inner.children.empty() || inner.children[0] == group) {
              continue;
            }
            if (inner.limit_offset >
                std::numeric_limits<size_t>::max() - expression.limit_offset) {
              continue;
            }
            const size_t offset = inner.limit_offset + expression.limit_offset;
            const bool empty = inner.limit_count != 0 &&
                               expression.limit_offset >= inner.limit_count;
            size_t count = 0;
            if (!empty) {
              if (inner.limit_count == 0) {
                count = expression.limit_count;
              } else if (expression.limit_count == 0) {
                count = inner.limit_count - expression.limit_offset;
              } else {
                count = std::min(expression.limit_count,
                                 inner.limit_count - expression.limit_offset);
              }
            }
            if (empty) {
              memo.AddExpression(
                  group, LogicalExpression{.operation = LogicalOperator::kEmpty,
                                           .children = inner.children});
            } else {
              memo.AddExpression(
                  group, LogicalExpression{.operation = LogicalOperator::kLimit,
                                           .children = inner.children,
                                           .limit_count = count,
                                           .limit_offset = offset});
            }
          }
        },
        LogicalOperator::kLimit));
    // Sort(Sort(R)) needs only one sort when the two key lists are compatible
    // prefixes.  Preserve the stronger order when the inner order already
    // satisfies the outer prefix; otherwise sort R directly by the outer
    // keys, which include the inner prefix.  NULL ordering is part of the
    // key identity, so incompatible explicit/default NULL placement is not
    // merged.
    built.Add(Rule(
        "sort_merge_of_compatible_orders", Sort(Sort(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& outer) {
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kSort ||
                inner.children.size() != 1 || inner.children[0] == group ||
                inner.target_list.size() != inner.sort_ascending.size() ||
                outer.target_list.size() != outer.sort_ascending.size()) {
              continue;
            }
            const auto same_key = [](const LogicalExpression& a, size_t ai,
                                     const LogicalExpression& b, size_t bi) {
              const std::optional<bool> a_null = ai < a.sort_nulls_first.size()
                                                     ? a.sort_nulls_first[ai]
                                                     : std::nullopt;
              const std::optional<bool> b_null = bi < b.sort_nulls_first.size()
                                                     ? b.sort_nulls_first[bi]
                                                     : std::nullopt;
              return a.target_list[ai].expression->ToString() ==
                         b.target_list[bi].expression->ToString() &&
                     a.sort_ascending[ai] == b.sort_ascending[bi] &&
                     a_null == b_null;
            };
            const auto is_prefix = [&](const LogicalExpression& prefix,
                                       const LogicalExpression& full) {
              if (prefix.target_list.size() > full.target_list.size()) {
                return false;
              }
              for (size_t i = 0; i < prefix.target_list.size(); ++i) {
                if (!same_key(prefix, i, full, i)) {
                  return false;
                }
              }
              return true;
            };
            if (is_prefix(outer, inner)) {
              LogicalExpression merged = inner;
              merged.children = inner.children;
              memo.AddExpression(group, std::move(merged));
            } else if (is_prefix(inner, outer)) {
              LogicalExpression merged = outer;
              merged.children = inner.children;
              memo.AddExpression(group, std::move(merged));
            }
          }
        },
        LogicalOperator::kSort));
    // Selection(true, X) ≡ X (FilterTrue). Copy child alternatives into this
    // group so costing can skip a residual filter.
    built.Add(Rule(
        "eliminate_true_selection", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate ||
              (*expression.predicate)->Type() != TypeTag::kConstantValue) {
            return;
          }
          const Value value =
              (*expression.predicate)->AsConstantValue().GetValue();
          if (value.IsNull() || !value.Truthy()) {
            return;
          }
          for (const LogicalExpression& child :
               memo.Get(bindings.at("input")).expressions) {
            bool refs_group = false;
            for (GroupId c : child.children) {
              if (c == group) {
                refs_group = true;
                break;
              }
            }
            if (refs_group) {
              continue;
            }
            memo.AddExpression(group, child);
          }
        },
        LogicalOperator::kSelection));
    // INTERSECT with an empty branch is empty regardless of the other
    // branches. EXCEPT with an empty left branch is empty as well. Materialize
    // the result as an Empty expression in the target group; the target
    // relation set can differ from the individual set-operation branches, so
    // the empty node receives a base group with the complete relation set.
    built.Add(Rule(
        "setop_empty_simplification", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const bool intersect =
              expression.operation == LogicalOperator::kIntersect ||
              expression.operation == LogicalOperator::kIntersectAll;
          const bool except =
              expression.operation == LogicalOperator::kExcept ||
              expression.operation == LogicalOperator::kExceptAll;
          if ((!intersect && !except) || expression.children.size() < 2) {
            return;
          }
          // INTERSECT empties when ANY branch is empty; EXCEPT only when
          // its LEFT branch is empty (A EXCEPT empty(B) is just A -- adding
          // an Empty alternative there would collapse a non-empty result).
          const bool empties =
              intersect
                  ? std::ranges::any_of(
                        expression.children,
                        [&](GroupId child) {
                          return std::ranges::any_of(
                              memo.Get(child).expressions,
                              [](const LogicalExpression& candidate) {
                                return candidate.operation ==
                                       LogicalOperator::kEmpty;
                              });
                        })
                  : except &&
                        std::ranges::any_of(
                            memo.Get(expression.children.front()).expressions,
                            [](const LogicalExpression& candidate) {
                              return candidate.operation ==
                                     LogicalOperator::kEmpty;
                            });
          if (!empties) {
            return;
          }
          const GroupId base = memo.EnsureGroup(memo.Get(group).relations);
          if (base == group) {
            return;
          }
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kEmpty,
                                       .children = {base}});
        },
        {LogicalOperator::kIntersect, LogicalOperator::kIntersectAll,
         LogicalOperator::kExcept, LogicalOperator::kExceptAll}));
    // Empty branches are identity elements for UNION ALL and for the right
    // side of EXCEPT ALL.  DISTINCT variants retain their duplicate-elision
    // contract by inserting a logical Distinct around the surviving branch.
    // Keeping these alternatives in the memo lets costing choose the cheap
    // branch without making the rewrite depend on a particular physical
    // implementation.
    built.Add(Rule(
        "setop_empty_identity", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const auto is_empty = [&](GroupId child) {
            return std::ranges::any_of(memo.Get(child).expressions,
                                       [](const LogicalExpression& candidate) {
                                         return candidate.operation ==
                                                LogicalOperator::kEmpty;
                                       });
          };
          const auto copy_child = [&](GroupId child) {
            for (const LogicalExpression& alternative :
                 memo.Get(child).expressions) {
              bool refs_group = false;
              for (GroupId c : alternative.children) {
                if (c == group) {
                  refs_group = true;
                  break;
                }
              }
              if (refs_group) {
                continue;
              }
              memo.AddExpression(group, alternative);
            }
          };
          if (expression.children.size() < 2) {
            return;
          }
          const bool has_empty =
              std::ranges::any_of(expression.children, is_empty);
          if (!has_empty) {
            return;
          }

          if (expression.operation == LogicalOperator::kUnionAll) {
            std::vector<GroupId> survivors;
            for (GroupId child : expression.children) {
              if (!is_empty(child)) {
                survivors.push_back(child);
              }
            }
            if (survivors.empty()) {
              // Every branch is empty: the union itself is empty (this is
              // how an all-empty GROUPING SETS / multi-branch plan folds).
              const GroupId base = memo.EnsureGroup(memo.Get(group).relations);
              if (base != group) {
                memo.AddExpression(
                    group,
                    LogicalExpression{.operation = LogicalOperator::kEmpty,
                                      .children = {base}});
              }
              return;
            }
            std::vector<std::string> survivor_relations;
            for (GroupId child : survivors) {
              survivor_relations =
                  UnionRelations(survivor_relations, memo.Get(child).relations);
            }
            if (survivor_relations != memo.Get(group).relations) {
              return;
            }
            if (survivors.size() == 1) {
              copy_child(survivors.front());
            } else if (survivors.size() >= 2) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                    .children = std::move(survivors)});
            }
            return;
          }

          if (expression.operation != LogicalOperator::kUnion &&
              expression.operation != LogicalOperator::kExceptAll &&
              expression.operation != LogicalOperator::kExcept) {
            return;
          }

          if (expression.operation == LogicalOperator::kExceptAll) {
            if (!is_empty(expression.children.back())) {
              return;
            }
            if (memo.Get(expression.children.front()).relations !=
                memo.Get(group).relations) {
              return;
            }
            copy_child(expression.children.front());
            return;
          }

          if (expression.operation == LogicalOperator::kExcept) {
            if (!is_empty(expression.children.back())) {
              return;
            }
            if (memo.Get(expression.children.front()).relations !=
                memo.Get(group).relations) {
              return;
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kDistinct,
                                  .children = {expression.children.front()}});
            return;
          }

          // UNION DISTINCT: an empty branch can be removed, but the result
          // still needs duplicate elimination.
          std::vector<GroupId> survivors;
          for (GroupId child : expression.children) {
            if (!is_empty(child)) {
              survivors.push_back(child);
            }
          }
          if (survivors.empty()) {
            // Distinct union of empty branches is empty.
            const GroupId base = memo.EnsureGroup(memo.Get(group).relations);
            if (base != group) {
              memo.AddExpression(
                  group, LogicalExpression{.operation = LogicalOperator::kEmpty,
                                           .children = {base}});
            }
            return;
          }
          std::vector<std::string> survivor_relations;
          for (GroupId child : survivors) {
            survivor_relations =
                UnionRelations(survivor_relations, memo.Get(child).relations);
          }
          if (survivor_relations != memo.Get(group).relations) {
            return;
          }
          if (survivors.size() == 1) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kDistinct,
                                  .children = {survivors.front()}});
            return;
          }
          const GroupId union_all = memo.EnsureDerivedGroup(
              memo.Get(group).relations,
              "setop-empty-union-all:" + GroupIdsTag(survivors));
          memo.AddExpression(
              union_all,
              LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                .children = survivors});
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {union_all}});
        },
        {LogicalOperator::kUnion, LogicalOperator::kUnionAll,
         LogicalOperator::kExcept, LogicalOperator::kExceptAll}));
    // UNION DISTINCT is equivalent to UNION ALL followed by duplicate
    // elimination. Keeping both forms in the memo lets the implementation
    // rules choose the direct set operator or the reusable append + distinct
    // pipeline independently.
    built.Add(Rule(
        "union_to_union_all_plus_distinct",
        Pattern::Op(LogicalOperator::kUnion, {}),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const GroupId union_all = memo.EnsureDerivedGroup(
              memo.Get(group).relations,
              "union-all-for-distinct:" + GroupIdsTag(expression.children));
          memo.AddExpression(
              union_all,
              LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                .children = expression.children});
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {union_all}});
        },
        LogicalOperator::kUnion));
    // Flatten nested UNION ALL branches into one n-ary append node.
    built.Add(Rule(
        "union_all_merge", Pattern::Op(LogicalOperator::kUnionAll, {}),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          std::vector<GroupId> flattened;
          bool changed = false;
          for (const GroupId child : expression.children) {
            const auto nested = std::ranges::find_if(
                memo.Get(child).expressions, [](const LogicalExpression& item) {
                  return item.operation == LogicalOperator::kUnionAll;
                });
            if (nested == memo.Get(child).expressions.end()) {
              flattened.push_back(child);
              continue;
            }
            flattened.insert(flattened.end(), nested->children.begin(),
                             nested->children.end());
            changed = true;
          }
          if (changed && flattened.size() >= 2) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                  .children = std::move(flattened)});
          }
        },
        LogicalOperator::kUnionAll));

    // push_filter_through_left_join_left_side: Selection(pred, OuterJoin(L, R))
    //   -> Selection(pred, OuterJoin(Selection(pred, L), R))
    // when pred references only columns from L and L is the preserved side
    // (join_type 0 = LEFT). Safe because the Selection on L filters rows
    // before the outer join, and unmatched L rows still get NULL-padded on
    // the right. For RIGHT/FULL outer joins L is the NULL-supplying side:
    // filtering it before the join would drop padded output rows, so the
    // rule must not fire there.
    built.Add(Rule(
        "push_filter_through_left_join_left_side",
        Selection(OuterJoin(Any("left"), Any("right"), "join")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          // Fixpoint guard: a join group this rule already built carries the
          // push; re-pushing above it would derive an endless chain of
          // filtered groups (each new Selection matches the pattern again).
          if (memo.Get(bindings.at("join"))
                  .tag.starts_with("left-join-filtered-join:")) {
            return;
          }
          const Expression pred = *expression.predicate;
          // Get the left child group's relations.
          const Group& left_group = memo.Get(bindings.at("left"));
          std::unordered_set<std::string> left_relations;
          for (const auto& rel : left_group.relations) {
            left_relations.insert(rel);
          }
          if (left_relations.empty()) {
            return;
          }
          // Only push if predicate references only left-side relations.
          if (!ReferencesOnly(pred, left_relations)) {
            return;
          }
          // Only the preserved (left) side may be filtered early. RIGHT and
          // FULL outer joins NULL-supply the left side, so dropping left rows
          // here would remove NULL-padded output rows from the result.  A
          // group may hold several outer-join alternatives (RIGHT is
          // normalized to a swapped LEFT, FULL is decomposed), and the left
          // scan group's filter is shared by all of them: refuse unless every
          // outer-join alternative has the same preserved left/NULL-supplying
          // right orientation, otherwise the pushed filter leaks into a
          // null-supplying side.
          std::unordered_set<std::string> right_relations;
          for (const auto& rel : memo.Get(bindings.at("right")).relations) {
            right_relations.insert(rel);
          }
          const auto same_relations =
              [](const std::vector<std::string>& actual,
                 const std::unordered_set<std::string>& expected) {
                if (actual.size() != expected.size()) {
                  return false;
                }
                return std::ranges::all_of(actual, [&](const std::string& rel) {
                  return expected.contains(rel);
                });
              };
          for (const LogicalExpression& join :
               memo.Get(bindings.at("join")).expressions) {
            if (join.operation != LogicalOperator::kOuterJoin ||
                join.children.size() != 2) {
              continue;
            }
            if (join.join_type != 0 ||
                !same_relations(memo.Get(join.children[0]).relations,
                                left_relations) ||
                !same_relations(memo.Get(join.children[1]).relations,
                                right_relations)) {
              return;
            }
          }
          const std::vector<LogicalExpression> guarded_joins = [&] {
            std::vector<LogicalExpression> kept;
            for (const LogicalExpression& join :
                 memo.Get(bindings.at("join")).expressions) {
              if (join.operation == LogicalOperator::kOuterJoin &&
                  join.join_type == 0) {
                kept.push_back(join);
              }
            }
            return kept;
          }();
          if (guarded_joins.empty()) {
            return;
          }
          // Snapshot the guarded join alternatives before mutating the join
          // group (guarded_joins above).
          // Selection(pred) filters the left input from a dedicated derived
          // group; adding it into the left group itself would be a
          // self-referencing expression.
          const GroupId filtered_left = memo.EnsureDerivedGroup(
              memo.Get(bindings.at("left")).relations,
              "left-join-filter:" + GroupIdsTag({bindings.at("left")}) +
                  pred->ToString());
          InheritBranchLocal(memo, filtered_left, group);
          if (filtered_left == bindings.at("left") || filtered_left == group) {
            return;
          }
          memo.AddExpression(
              filtered_left,
              LogicalExpression{.operation = LogicalOperator::kSelection,
                                .children = {bindings.at("left")},
                                .predicate = pred});
          // The filtered outer join gets its OWN derived group.  Adding it to
          // the shared join group itself strengthened every consumer of that
          // group with this Selection's predicate: an or_to_union split puts
          // one Selection per branch over the same join group, so each
          // branch's partition predicate leaked into all the others and the
          // union lost every row but one branch's.
          const GroupId filtered_join = memo.EnsureDerivedGroup(
              memo.Get(bindings.at("join")).relations,
              "left-join-filtered-join:" +
                  GroupIdsTag({bindings.at("join"), filtered_left}) +
                  pred->ToString());
          InheritBranchLocal(memo, filtered_join, group);
          if (filtered_join == bindings.at("join") || filtered_join == group) {
            return;
          }
          for (const LogicalExpression& join : guarded_joins) {
            // Rebuild the outer join with the filtered left input.  The join
            // condition stays the original one: the Selection predicate is a
            // row filter on the left side, not an ON-clause conjunct.
            memo.AddExpression(
                filtered_join,
                LogicalExpression{
                    .operation = LogicalOperator::kOuterJoin,
                    .children = {filtered_left, bindings.at("right")},
                    .predicate = join.predicate,
                    .target_list = join.target_list,
                    .join_type = join.join_type,
                    .output_schema = join.output_schema});
          }
          // Wrap with the original Selection (idempotent, keeps the plan
          // shape stable while the filtered join carries the push).
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {filtered_join},
                                       .predicate = pred});
        },
        LogicalOperator::kSelection));

    // join_on_false_to_empty: Join(L, R, FALSE/NULL predicate) -> Empty.
    // When the join predicate is a constant FALSE or NULL, no rows can match.
    // The empty alternative preserves the group's relation set (a Limit over
    // the left child alone would fail relation validation and drop the right
    // side's schema).
    built.Add(Rule(
        "join_on_false_to_empty", Join(Any("left"), Any("right")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          const Expression pred = *expression.predicate;
          if (pred->Type() != TypeTag::kConstantValue) {
            return;
          }
          const Value val = pred->AsConstantValue().GetValue();
          if (!val.IsNull() && val.Truthy()) {
            return;
          }
          // Predicate is FALSE or NULL: the join yields no rows.
          const GroupId base = memo.EnsureGroup(memo.Get(group).relations);
          if (base == group) {
            return;
          }
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kEmpty,
                                       .children = {base}});
        },
        LogicalOperator::kJoin));

    // eliminate_double_sort: Sort(Sort(X)) -> Sort(X). If the inner Sort
    // already orders by the same or a superset of keys, the outer Sort is
    // redundant.
    built.Add(Rule(
        "eliminate_double_sort", Sort(Sort(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kSort) {
              continue;
            }
            // D5 gate: the outer Sort is redundant only when the inner Sort
            // produces the same ordering for every key the outer consumer
            // needs. Check ALL of: key expressions, ascending direction,
            // and NULLS FIRST/LAST.
            const size_t outer_keys = expression.target_list.size();
            const size_t inner_keys = inner.target_list.size();
            if (outer_keys == 0 || outer_keys > inner_keys) {
              continue;
            }
            bool keys_match = true;
            for (size_t i = 0; i < outer_keys; ++i) {
              if (expression.target_list[i].expression->ToString() !=
                  inner.target_list[i].expression->ToString()) {
                keys_match = false;
                break;
              }
            }
            if (!keys_match) {
              continue;
            }
            // Check ascending direction prefix.
            bool dir_match = true;
            for (size_t i = 0; i < outer_keys; ++i) {
              if (i >= expression.sort_ascending.size() ||
                  i >= inner.sort_ascending.size() ||
                  expression.sort_ascending[i] != inner.sort_ascending[i]) {
                dir_match = false;
                break;
              }
            }
            if (!dir_match) {
              continue;
            }
            // Check NULLS FIRST/LAST prefix.
            bool nulls_match = true;
            for (size_t i = 0; i < outer_keys; ++i) {
              if (i >= expression.sort_nulls_first.size() ||
                  i >= inner.sort_nulls_first.size() ||
                  expression.sort_nulls_first[i] != inner.sort_nulls_first[i]) {
                nulls_match = false;
                break;
              }
            }
            if (!nulls_match) {
              continue;
            }
            bool refs_group = false;
            for (GroupId c : inner.children) {
              if (c == group) {
                refs_group = true;
                break;
              }
            }
            if (refs_group) {
              continue;
            }
            // The inner Sort provides the same ordering; remove the outer.
            memo.AddExpression(group, inner);
            return;
          }
        },
        LogicalOperator::kSort));

    // distinct_over_group_by: Distinct(Aggregate(...GROUP BY keys...)) ->
    //   Aggregate(...GROUP BY keys...) since GROUP BY already eliminates
    //   duplicates by the grouping keys.
    built.Add(Rule(
        "distinct_over_group_by", Distinct(Aggregation(Any(), "agg")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          (void)expression;
          const Group& agg_group = memo.Get(bindings.at("agg"));
          for (const LogicalExpression& agg : agg_group.expressions) {
            if (agg.operation != LogicalOperator::kAggregation) {
              continue;
            }
            bool refs_group = false;
            for (GroupId c : agg.children) {
              if (c == group) {
                refs_group = true;
                break;
              }
            }
            if (refs_group) {
              continue;
            }
            // DISTINCT is redundant over GROUP BY ONLY when the projected
            // targets include all grouping keys. Aggregates like COUNT(*) can
            // produce duplicate values across groups, so DISTINCT must not be
            // eliminated.
            if (agg.grouping_sets.empty()) {
              continue;
            }
            std::unordered_set<std::string> grouping_cols;
            for (const auto& g : agg.grouping_sets) {
              if (g && g->Type() == TypeTag::kColumnValue) {
                grouping_cols.insert(
                    g->AsColumnValue().GetColumnName().ToString());
                grouping_cols.insert(g->AsColumnValue().GetColumnName().name);
              }
            }
            std::unordered_set<std::string> proj_cols;
            for (const auto& target : agg.target_list) {
              if (target.expression &&
                  target.expression->Type() == TypeTag::kColumnValue) {
                proj_cols.insert(target.expression->AsColumnValue()
                                     .GetColumnName()
                                     .ToString());
                proj_cols.insert(
                    target.expression->AsColumnValue().GetColumnName().name);
              }
            }
            bool all_grouping_projected = !grouping_cols.empty();
            for (const auto& gc : grouping_cols) {
              if (!proj_cols.contains(gc)) {
                all_grouping_projected = false;
                break;
              }
            }
            if (!all_grouping_projected) {
              continue;
            }
            memo.AddExpression(group, agg);
            return;
          }
        },
        LogicalOperator::kDistinct));

    // push_projection_through_aggregation: Projection(Aggregate(X)) ->
    //   Aggregate(Projection(X)) when projection only references grouping
    //   keys and aggregate results. This is a conservative version that
    //   only fires when all projection targets are simple column references.
    built.Add(Rule(
        "push_projection_through_aggregation",
        Projection(Aggregation(Any(), "agg")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.target_list.empty()) {
            return;
          }
          for (const auto& target : expression.target_list) {
            if (!target.expression ||
                target.expression->Type() != TypeTag::kColumnValue) {
              return;
            }
          }
          const GroupId agg_id = bindings.at("agg");
          if (agg_id == group) {
            return;
          }
          const Group& agg_group = memo.Get(agg_id);
          for (const LogicalExpression& agg : agg_group.expressions) {
            if (agg.operation != LogicalOperator::kAggregation ||
                agg.children.empty() || agg.children[0] == agg_id ||
                agg.children[0] == group) {
              continue;
            }
            const GroupId agg_child = agg.children[0];
            // Fixpoint guard: the aggregation input is already this rule's
            // pruning projection; pruning it again only nests projections.
            if (memo.Get(agg_child).tag.starts_with("proj_below_agg:")) {
              continue;
            }
            // The derived-group tag must identify the meaning, not just the
            // rule: a bare count-style tag would collide across distinct
            // (outer, aggregation) target lists over the same relations and
            // pollute one group with another site's projection.
            // ...and the input: the per-branch aggregates of a union split
            // share both target lists, so without the child id every
            // branch's pushed projection landed in one group.
            std::string tag = "proj_below_agg:" + GroupIdsTag({agg_child});
            for (const NamedExpression& target : expression.target_list) {
              tag += target.name;
              tag += '=';
              tag += target.expression ? target.expression->ToString() : "?";
              tag += ';';
            }
            tag += '|';
            for (const NamedExpression& target : agg.target_list) {
              tag += target.name;
              tag += '=';
              tag += target.expression ? target.expression->ToString() : "?";
              tag += ';';
            }
            std::unordered_set<ColumnName> needed_cols;
            for (const auto& g : agg.grouping_sets) {
              if (g) {
                for (const auto& c : g->TouchedColumns()) {
                  needed_cols.insert(c);
                }
              }
            }
            for (const auto& t : agg.target_list) {
              if (t.expression) {
                for (const auto& c : t.expression->TouchedColumns()) {
                  needed_cols.insert(c);
                }
              }
            }
            std::vector<NamedExpression> below_targets;
            below_targets.reserve(needed_cols.size());
            for (const auto& col : needed_cols) {
              below_targets.emplace_back(col.name, ColumnValueExp(col));
            }

            const GroupId proj_below =
                memo.EnsureDerivedGroup(memo.Get(agg_child).relations, tag);
            const GroupId new_agg = memo.EnsureDerivedGroup(
                memo.Get(agg_child).relations, tag + "|agg");
            if (proj_below != agg_child && proj_below != group &&
                new_agg != agg_id && new_agg != group) {
              memo.AddExpression(
                  proj_below,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {agg_child},
                                    .target_list = std::move(below_targets)});
              memo.AddExpression(
                  new_agg,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {proj_below},
                                    .target_list = agg.target_list,
                                    .output_schema = agg.output_schema,
                                    .partition_by = agg.partition_by,
                                    .grouping_sets = agg.grouping_sets});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {new_agg},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            }
            return;
          }
        },
        LogicalOperator::kProjection));

    // limit_push_through_sort: Limit(Sort(X)) -> TopN(X).  LIMIT n OVER
    // ORDER BY means "the n smallest"; a Limit pushed BELOW the sort would
    // instead mean "sort an arbitrary n input rows" -- a cheaper but WRONG
    // plan the cost model would prefer.  TopN is the sound fusion, so the
    // rule now only ever produces that.  OFFSET cannot be pushed below the
    // sort (it must skip post-sort rows), so offset != 0 does not fire.
    built.Add(Rule(
        "limit_push_through_sort", Limit(Sort(Any(), "sort")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.limit_count == 0 || expression.limit_offset != 0) {
            return;
          }
          const GroupId sort_group_id = bindings.at("sort");
          const Group& sort_group = memo.Get(sort_group_id);
          for (const LogicalExpression& sort : sort_group.expressions) {
            if (sort.operation != LogicalOperator::kSort ||
                sort.children.size() != 1 || sort.children[0] == group) {
              continue;
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kTopN,
                                  .children = sort.children,
                                  .target_list = sort.target_list,
                                  .sort_ascending = sort.sort_ascending,
                                  .sort_nulls_first = sort.sort_nulls_first,
                                  .limit_count = expression.limit_count,
                                  .limit_offset = 0,
                                  .with_ties = expression.with_ties,
                                  .output_schema = sort.output_schema});
            return;
          }
        },
        LogicalOperator::kLimit));

    // limit_prefix_{unordered,ordered}_window: the safe exception for
    // pushing a Limit below a Window, which the general push cannot do
    // (push_limit_through_window - a window is row-preserving, so a
    // truncated input changes partition-relative computations).
    //
    // The engine's window emits rows in child order and scatters per-row
    // values back to their original positions, and a Limit truncates the
    // emitted stream at a fixed prefix.  So Window(Limit(X)) keeps exactly
    // the rows Limit(Window(X)) emits, and every surviving row's value is
    // unchanged when (a) the window call reads only rows at or before the
    // survivor in partition order (ranking calls; explicit ROWS frames
    // bounded by CURRENT ROW) and (b) the truncated suffix cannot hold a
    // row that earlier in partition order either by having no order keys at
    // all (partition order IS input order) or by a child sort that
    // implements the spec exactly, fused into a TopN.  WITH TIES keeps peers
    // of the boundary row and is refused; OFFSET is a wider prefix cut and
    // is allowed.  A nested window layer under the transformed one is
    // refused (its stability would need the same gate recursively).
    built.Add(Rule(
        "limit_prefix_unordered_window", Limit(Window(Any("input"), "window")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.with_ties || expression.limit_count == 0) {
            return;
          }
          const GroupId window_group_id = bindings.at("window");
          if (window_group_id == group ||
              memo.Get(window_group_id)
                  .tag.starts_with("window-prefix-window:")) {
            return;
          }
          for (const LogicalExpression& window :
               memo.Get(window_group_id).expressions) {
            if (window.operation != LogicalOperator::kWindow ||
                window.children.size() != 1 || window.children[0] == group) {
              continue;
            }
            const GroupId input_id = window.children[0];
            const auto gate = WindowPrefixGate(window);
            if (!gate.has_value() || !gate->stable || gate->order_terms != 0 ||
                memo.Get(input_id).tag.starts_with("window-prefix-limit:") ||
                ReachableChildHasWindow(memo, input_id)) {
              continue;
            }
            const std::string signature =
                GroupIdsTag({input_id}) + gate->spec +
                std::to_string(expression.limit_count) + ":" +
                std::to_string(expression.limit_offset);
            const GroupId limited =
                memo.EnsureDerivedGroup(memo.Get(input_id).relations,
                                        "window-prefix-limit:" + signature);
            if (limited == group || limited == input_id) {
              continue;
            }
            // The inner cut is a PREFIX: the window must still number the
            // OFFSET rows the outer Limit later skips, so keep offset+count
            // rows from the start and let only the outer Limit apply OFFSET.
            size_t prefix_rows = 0;
            if (__builtin_add_overflow(expression.limit_count,
                                       expression.limit_offset,
                                       &prefix_rows)) {
              continue;
            }
            memo.AddExpression(
                limited,
                LogicalExpression{.operation = LogicalOperator::kLimit,
                                  .children = {input_id},
                                  .limit_count = prefix_rows,
                                  .limit_offset = 0});
            const GroupId windowed =
                memo.EnsureDerivedGroup(memo.Get(input_id).relations,
                                        "window-prefix-window:" + signature);
            if (windowed == group) {
              continue;
            }
            memo.AddExpression(
                windowed,
                LogicalExpression{.operation = LogicalOperator::kWindow,
                                  .children = {limited},
                                  .target_list = window.target_list,
                                  .output_schema = window.output_schema,
                                  .partition_by = window.partition_by});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kLimit,
                                  .children = {windowed},
                                  .limit_count = expression.limit_count,
                                  .limit_offset = expression.limit_offset});
          }
        },
        LogicalOperator::kLimit));

    built.Add(Rule(
        "limit_prefix_ordered_window",
        Limit(Window(Sort(Any(), "sort"), "window")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.with_ties || expression.limit_count == 0) {
            return;
          }
          const GroupId sort_group_id = bindings.at("sort");
          if (sort_group_id == group ||
              memo.Get(sort_group_id).tag.starts_with("window-prefix-limit:")) {
            return;
          }
          const GroupId window_group_id = bindings.at("window");
          if (window_group_id == group ||
              memo.Get(window_group_id)
                  .tag.starts_with("window-prefix-window:")) {
            return;
          }
          for (const LogicalExpression& window :
               memo.Get(window_group_id).expressions) {
            if (window.operation != LogicalOperator::kWindow ||
                window.children.size() != 1 ||
                window.children[0] != sort_group_id) {
              continue;
            }
            const auto gate = WindowPrefixGate(window);
            if (!gate.has_value() || !gate->stable || gate->order_terms == 0 ||
                ReachableChildHasWindow(memo, sort_group_id)) {
              continue;
            }
            const WindowFunctionCallExpression* reference = nullptr;
            for (const NamedExpression& target : window.target_list) {
              if (target.expression) {
                reference =
                    &target.expression->AsWindowFunctionCallExpression();
                break;
              }
            }
            if (reference == nullptr) {
              continue;
            }
            const LogicalExpression* sort_node = nullptr;
            for (const LogicalExpression& sort :
                 memo.Get(sort_group_id).expressions) {
              if (sort.operation == LogicalOperator::kSort &&
                  sort.children.size() == 1 && sort.children[0] != group &&
                  SortDeliversWindowSpec(sort, *reference)) {
                sort_node = &sort;
                break;
              }
            }
            if (sort_node == nullptr) {
              continue;
            }
            const std::string signature =
                GroupIdsTag({window_group_id, sort_group_id}) + gate->spec +
                std::to_string(expression.limit_count) + ":" +
                std::to_string(expression.limit_offset);
            const GroupId limited =
                memo.EnsureDerivedGroup(memo.Get(window_group_id).relations,
                                        "window-prefix-limit:" + signature);
            if (limited == group || limited == window_group_id ||
                limited == sort_group_id) {
              continue;
            }
            // Prefix cut: keep offset+count rows; OFFSET is applied once, by
            // the outer Limit (see limit_prefix_unordered_window).
            size_t prefix_rows = 0;
            if (__builtin_add_overflow(expression.limit_count,
                                       expression.limit_offset,
                                       &prefix_rows)) {
              continue;
            }
            memo.AddExpression(
                limited, LogicalExpression{
                             .operation = LogicalOperator::kTopN,
                             .children = sort_node->children,
                             .target_list = sort_node->target_list,
                             .sort_ascending = sort_node->sort_ascending,
                             .sort_nulls_first = sort_node->sort_nulls_first,
                             .limit_count = prefix_rows,
                             .limit_offset = 0,
                             .with_ties = false});
            const GroupId windowed =
                memo.EnsureDerivedGroup(memo.Get(window_group_id).relations,
                                        "window-prefix-window:" + signature);
            if (windowed == group) {
              continue;
            }
            memo.AddExpression(
                windowed,
                LogicalExpression{.operation = LogicalOperator::kWindow,
                                  .children = {limited},
                                  .target_list = window.target_list,
                                  .output_schema = window.output_schema,
                                  .partition_by = window.partition_by});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kLimit,
                                  .children = {windowed},
                                  .limit_count = expression.limit_count,
                                  .limit_offset = expression.limit_offset});
          }
        },
        LogicalOperator::kLimit));

    // push_filter_through_sort: Selection(Sort(X)) -> Sort(Selection(X))
    // when the selection predicate references only the child's columns.
    // The "references only the child's columns" side holds unconditionally:
    // Sort is schema-preserving (its output schema is the child's), so any
    // column the predicate can name is already available below the sort.
    // No explicit check is needed, and adding one would only reject
    // constant-only predicates.
    built.Add(Rule(
        "push_filter_through_sort", Selection(Sort(Any(), "sort")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          const GroupId sort_group_id = bindings.at("sort");
          const Group& sort_group = memo.Get(sort_group_id);
          for (const LogicalExpression& sort : sort_group.expressions) {
            if (sort.operation != LogicalOperator::kSort ||
                sort.children.empty()) {
              continue;
            }
            const GroupId input_id = sort.children[0];
            const GroupId filtered = memo.EnsureDerivedGroup(
                memo.Get(input_id).relations,
                "filter-before-sort:" + GroupIdsTag({input_id}) +
                    (*expression.predicate)->ToString());
            InheritBranchLocal(memo, filtered, group);
            if (filtered == group || filtered == input_id) {
              continue;
            }
            memo.AddExpression(
                filtered,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {input_id},
                                  .predicate = expression.predicate});
            // Sort wraps the Selection.
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSort,
                                  .children = {filtered},
                                  .target_list = sort.target_list,
                                  .sort_ascending = sort.sort_ascending,
                                  .sort_nulls_first = sort.sort_nulls_first,
                                  .output_schema = sort.output_schema});
            return;
          }
        },
        LogicalOperator::kSelection));

    // eliminate_sort_under_unordered_consumer: Aggregation(Sort(X)) ->
    // Aggregation(X) when the aggregation has no order-sensitive
    // requirements. The payload (grouping sets, partition, schema) is
    // copied verbatim; dropping the grouping would turn a GROUP BY
    // aggregate into a scalar one, and order-sensitive aggregates
    // (string_agg / array_agg / WITHIN GROUP / ANY_VALUE / sketches) could
    // observe a different input order after the Sort disappears.
    built.Add(Rule(
        "eliminate_sort_under_unordered_consumer",
        Aggregation(Sort(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const auto order_insensitive = [](const LogicalExpression& agg) {
            return std::ranges::all_of(
                agg.target_list, [](const NamedExpression& target) {
                  if (!target.expression ||
                      target.expression->Type() != TypeTag::kAggregateExp) {
                    return true;
                  }
                  const auto& aggregate =
                      static_cast<const AggregateExpression&>(
                          *target.expression);
                  if (!aggregate.InnerOrderBy().empty()) {
                    return false;  // WITHIN GROUP orders its own input.
                  }
                  switch (aggregate.GetType()) {
                    case AggregationType::kCount:
                    case AggregationType::kSum:
                    case AggregationType::kAvg:
                    case AggregationType::kMin:
                    case AggregationType::kMax:
                    case AggregationType::kLogicalAnd:
                    case AggregationType::kLogicalOr:
                    case AggregationType::kCountIf:
                    case AggregationType::kVarSamp:
                    case AggregationType::kVarPop:
                    case AggregationType::kStddevSamp:
                    case AggregationType::kStddevPop:
                    case AggregationType::kCovarSamp:
                    case AggregationType::kCovarPop:
                    case AggregationType::kCorr:
                    case AggregationType::kBitAnd:
                    case AggregationType::kBitOr:
                    case AggregationType::kBitXor:
                    case AggregationType::kApproxCountDistinct:
                    case AggregationType::kMode:
                    case AggregationType::kRegrSlope:
                    case AggregationType::kRegrIntercept:
                    case AggregationType::kRegrCount:
                    case AggregationType::kRegrR2:
                    case AggregationType::kRegrAvgX:
                    case AggregationType::kRegrAvgY:
                    case AggregationType::kRegrSxx:
                    case AggregationType::kRegrSyy:
                    case AggregationType::kRegrSxy:
                      return true;
                    default:
                      return false;
                  }
                });
          };
          if (!order_insensitive(expression)) {
            return;
          }
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kSort ||
                inner.children.empty() || inner.children[0] == group) {
              continue;
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kAggregation,
                                  .children = inner.children,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema,
                                  .partition_by = expression.partition_by,
                                  .grouping_sets = expression.grouping_sets});
          }
        },
        LogicalOperator::kAggregation));

    // distinct_over_distinct: Distinct(Distinct(X)) -> Distinct(X)
    built.Add(Rule(
        "distinct_over_distinct", Distinct(Distinct(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          (void)expression;
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kDistinct ||
                inner.children.empty() || inner.children[0] == group) {
              continue;
            }
            memo.AddExpression(group, inner);
            return;
          }
        },
        LogicalOperator::kDistinct));

    // cross_to_inner_with_predicate: Selection(CrossJoin(L, R), p) ->
    // Join(L, R, p).  Sound for ANY p: sigma_p(L x R) is definitionally the
    // inner join on p, whether or not p references both sides (a one-sided
    // or constant p just makes a trivial join condition; pushdown rules
    // handle it from there).  No both-sides inspection is needed.
    built.Add(Rule(
        "cross_to_inner_with_predicate",
        Selection(CrossJoin(Any("left"), Any("right"))),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kJoin,
                                       .children = {bindings.at("left"),
                                                    bindings.at("right")},
                                       .predicate = expression.predicate});
        },
        LogicalOperator::kSelection));

    // join_empty_simplification: Join/CrossJoin with an Empty child -> Empty
    built.Add(Rule(
        "join_empty_simplification", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kJoin &&
              expression.operation != LogicalOperator::kCrossJoin &&
              expression.operation != LogicalOperator::kSemiJoin &&
              expression.operation != LogicalOperator::kAntiJoin) {
            return;
          }
          if (expression.children.size() < 2) {
            return;
          }
          const auto is_empty = [&](GroupId child) {
            return std::ranges::any_of(memo.Get(child).expressions,
                                       [](const LogicalExpression& candidate) {
                                         return candidate.operation ==
                                                LogicalOperator::kEmpty;
                                       });
          };
          if (is_empty(expression.children[0]) ||
              (expression.operation != LogicalOperator::kAntiJoin &&
               is_empty(expression.children[1]))) {
            const GroupId base = memo.EnsureGroup(memo.Get(group).relations);
            if (base != group) {
              memo.AddExpression(
                  group, LogicalExpression{.operation = LogicalOperator::kEmpty,
                                           .children = {base}});
            }
          }
        },
        {LogicalOperator::kJoin, LogicalOperator::kCrossJoin,
         LogicalOperator::kSemiJoin, LogicalOperator::kAntiJoin}));

    // intersect_to_semijoin: Intersect(L, R) -> SemiJoin(L, R,
    // equality_on_all_columns)
    built.Add(Rule(
        "intersect_to_semijoin", Pattern::Op(LogicalOperator::kIntersect, {}),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kIntersect ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId left = expression.children[0];
          const GroupId right = expression.children[1];
          const Group& left_group = memo.Get(left);
          const Group& right_group = memo.Get(right);
          std::vector<std::string> intersection;
          std::ranges::set_intersection(left_group.relations,
                                        right_group.relations,
                                        std::back_inserter(intersection));
          if (!intersection.empty() ||
              UnionRelations(left_group.relations, right_group.relations) !=
                  memo.Get(group).relations) {
            return;
          }
          const Expression equality_predicate =
              BuildEqualityOnAllColumns(memo, expression, left, right);
          if (!equality_predicate) {
            return;
          }
          // INTERSECT is a DISTINCT set operation: a bare semijoin would
          // keep left-side multiplicity (L={(1),(1)}, R={(1)} must yield one
          // row). Wrap the semijoin in a logical Distinct.
          const std::string tag =
              "intersect-semi:" +
              TargetListFingerprint(expression.target_list) + ":" +
              (expression.predicate && *expression.predicate
                   ? (*expression.predicate)->ToString()
                   : std::string("null"));
          const GroupId join_group =
              memo.EnsureDerivedGroup(memo.Get(group).relations, tag);
          if (join_group == group || join_group == left ||
              join_group == right) {
            return;
          }
          memo.AddExpression(
              join_group,
              LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                .children = {left, right},
                                .predicate = equality_predicate,
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {join_group}});
        },
        LogicalOperator::kIntersect));

    // except_to_antijoin: Except(L, R) -> AntiJoin(L, R,
    // equality_on_all_columns)
    built.Add(Rule(
        "except_to_antijoin", Pattern::Op(LogicalOperator::kExcept, {}),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kExcept ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId left = expression.children[0];
          const GroupId right = expression.children[1];
          const Group& left_group = memo.Get(left);
          const Group& right_group = memo.Get(right);
          std::vector<std::string> intersection;
          std::ranges::set_intersection(left_group.relations,
                                        right_group.relations,
                                        std::back_inserter(intersection));
          if (!intersection.empty() ||
              UnionRelations(left_group.relations, right_group.relations) !=
                  memo.Get(group).relations) {
            return;
          }
          const Expression equality_predicate =
              BuildEqualityOnAllColumns(memo, expression, left, right);
          if (!equality_predicate) {
            return;
          }
          // EXCEPT is DISTINCT as well: the antijoin keeps every left-side
          // duplicate, so wrap it in a logical Distinct.
          const std::string tag =
              "except-anti:" + TargetListFingerprint(expression.target_list) +
              ":" +
              (expression.predicate && *expression.predicate
                   ? (*expression.predicate)->ToString()
                   : std::string("null"));
          const GroupId join_group =
              memo.EnsureDerivedGroup(memo.Get(group).relations, tag);
          if (join_group == group || join_group == left ||
              join_group == right) {
            return;
          }
          memo.AddExpression(
              join_group,
              LogicalExpression{.operation = LogicalOperator::kAntiJoin,
                                .children = {left, right},
                                .predicate = equality_predicate,
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {join_group}});
        },
        LogicalOperator::kExcept));

    // distinct_and_group_by_interchange: Convert Distinct(Projection(X)) to
    // Aggregation(X, GROUP BY projection_columns) as an alternative
    // representation for hash aggregation.
    built.Add(Rule(
        "distinct_and_group_by_interchange", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation == LogicalOperator::kDistinct &&
              expression.children.size() == 1) {
            const Group& inner_group = memo.Get(expression.children[0]);
            for (const LogicalExpression& inner : inner_group.expressions) {
              if (inner.operation != LogicalOperator::kProjection ||
                  inner.children.size() != 1 || inner.children[0] == group ||
                  inner.target_list.empty()) {
                continue;
              }
              std::vector<Expression> grouping;
              grouping.reserve(inner.target_list.size());
              for (const auto& item : inner.target_list) {
                grouping.push_back(item.expression);
              }
              LogicalExpression aggregation;
              aggregation.operation = LogicalOperator::kAggregation;
              aggregation.children = inner.children;
              aggregation.target_list = inner.target_list;
              aggregation.output_schema = inner.output_schema;
              aggregation.grouping_sets = std::move(grouping);
              memo.AddExpression(group, std::move(aggregation));
            }
          } else if (expression.operation == LogicalOperator::kAggregation &&
                     expression.children.size() == 1 &&
                     !expression.target_list.empty() &&
                     !expression.grouping_sets.empty()) {
            bool has_agg = false;
            for (const auto& item : expression.target_list) {
              if (ContainsAggregate(item.expression)) {
                has_agg = true;
                break;
              }
            }
            if (!has_agg) {
              // Only interchange back to Distinct(Projection(X)) when all
              // grouping keys survive the projection; otherwise the Distinct
              // would not deduplicate rows that differ on a dropped key.
              std::unordered_set<std::string> grouping_cols;
              for (const auto& g : expression.grouping_sets) {
                if (g && g->Type() == TypeTag::kColumnValue) {
                  grouping_cols.insert(
                      g->AsColumnValue().GetColumnName().ToString());
                  grouping_cols.insert(g->AsColumnValue().GetColumnName().name);
                }
              }
              std::unordered_set<std::string> proj_cols;
              for (const auto& target : expression.target_list) {
                if (target.expression &&
                    target.expression->Type() == TypeTag::kColumnValue) {
                  proj_cols.insert(target.expression->AsColumnValue()
                                       .GetColumnName()
                                       .ToString());
                  proj_cols.insert(
                      target.expression->AsColumnValue().GetColumnName().name);
                }
              }
              bool all_grouping_projected = !grouping_cols.empty();
              for (const auto& gc : grouping_cols) {
                if (!proj_cols.contains(gc)) {
                  all_grouping_projected = false;
                  break;
                }
              }
              if (!all_grouping_projected) {
                return;
              }
              const GroupId child = expression.children[0];
              // Content-addressed (D1): a tag carrying only the target
              // count let every group-by-only Aggregation over the same
              // relation set share one projection group -- e.g. the two
              // per-branch aggregates aggregate_union_transpose builds over
              // an or_to_union split -- so each branch's Distinct could pick
              // the OTHER branch's rows and the union lost one partition.
              const GroupId proj_group = memo.EnsureDerivedGroup(
                  memo.Get(child).relations,
                  "distinct-group-by-proj:" + std::to_string(child) + ":" +
                      TargetListFingerprint(expression.target_list));
              if (proj_group == group || proj_group == child) {
                return;
              }
              memo.AddExpression(
                  proj_group,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {child},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kDistinct,
                                    .children = {proj_group}});
            }
          }
        },
        {LogicalOperator::kDistinct, LogicalOperator::kAggregation}));

    // self_join_elimination: When Join(T1, T2) joins the same table on its
    // primary key / unique key with identical predicates and schema projection,
    // eliminate redundant scan.
    built.Add(Rule(
        "self_join_elimination", Join(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kJoin ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId left_id = bindings.at("left");
          const GroupId right_id = bindings.at("right");
          const Group& left_group = memo.Get(left_id);
          const Group& right_group = memo.Get(right_id);
          if (left_group.relations.size() != 1 ||
              right_group.relations.size() != 1) {
            return;
          }
          const std::string& r1 = left_group.relations.front();
          const std::string& r2 = right_group.relations.front();

          std::string t1 = r1;
          std::string t2 = r2;
          for (const auto& expr : left_group.expressions) {
            if (expr.operation == LogicalOperator::kScan &&
                !expr.table.empty()) {
              t1 = expr.table;
              break;
            }
          }
          for (const auto& expr : right_group.expressions) {
            if (expr.operation == LogicalOperator::kScan &&
                !expr.table.empty()) {
              t2 = expr.table;
              break;
            }
          }

          // Require an exact table match here, not the alias-stripping
          // IsSameTable: distinct real tables such as `orders` and
          // `orders_1` must never be conflated (the alias forms t1_1/t1_2
          // still match because both scans name the same real table).
          if (t1 != t2) {
            return;
          }

          if (!AreFiltersEquivalent(left_group.filter, r1, right_group.filter,
                                    r2)) {
            return;
          }

          // The join condition must be EXACTLY a key equality whose column
          // carries a UNIQUE/PRIMARY KEY constraint in the (shared) table
          // schema; otherwise the join multiplies rows and eliminating it
          // would change results.  Uniqueness is looked up under the left
          // relation's name: both sides scan the same table.
          if (!SingleUniqueKeyEquality(
                  memo, expression.predicate.value_or(Expression()), r1, r2,
                  r1)) {
            return;
          }
          // The replacement is a single scan of the left table: the join's
          // own target list must not reference the eliminated right side.
          for (const NamedExpression& target : expression.target_list) {
            if (!target.expression) {
              continue;
            }
            for (const ColumnName& column :
                 target.expression->TouchedColumns()) {
              if (!column.schema.empty() && column.schema == r2) {
                return;
              }
            }
          }

          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kScan,
                                .table = t1,
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
        },
        LogicalOperator::kJoin));

    // unique_semi_to_inner: Convert SemiJoin(L, R, p) to InnerJoin(L, R, p)
    // when right side keys are unique (each left row then has at most one
    // match, so inner and semi keep the same multiplicity).  Uniqueness is
    // proven from the catalog-published schema: the predicate must be
    // exactly the key equality and the right-side column must carry a
    // UNIQUE/PRIMARY KEY constraint.
    built.Add(Rule(
        "unique_semi_to_inner", SemiJoin(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSemiJoin ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId left_id = bindings.at("left");
          const GroupId right_id = bindings.at("right");
          const Group& left_group = memo.Get(left_id);
          const Group& right_group = memo.Get(right_id);
          if (expression.predicate) {
            std::string r_left = left_group.relations.empty()
                                     ? ""
                                     : left_group.relations.front();
            std::string r_right = right_group.relations.empty()
                                      ? ""
                                      : right_group.relations.front();
            if (SingleUniqueKeyEquality(memo, *expression.predicate, r_left,
                                        r_right, r_right)) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {left_id, right_id},
                                    .predicate = expression.predicate,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            }
          }
        },
        LogicalOperator::kSemiJoin));

    // outer_to_anti_join: Convert LeftOuterJoin(L, R, p) followed by
    // Selection(R.key IS NULL) into AntiJoin(L, R, p).
    built.Add(Rule(
        "outer_to_anti_join", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const Group& input_group = memo.Get(input_id);
          for (const LogicalExpression& join_expr : input_group.expressions) {
            if (join_expr.operation != LogicalOperator::kOuterJoin ||
                join_expr.children.size() != 2 ||
                join_expr.join_type != 0) {  // 0 = LeftOuter
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            const Group& right_group = memo.Get(right_id);
            const std::unordered_set<std::string> right_rels(
                right_group.relations.begin(), right_group.relations.end());
            auto resolves_to_right = [&](const ColumnName& col) {
              return right_rels.contains(col.schema);
            };

            // The Selection must be EXACTLY `right.col IS NULL` conjuncts:
            // the AntiJoin replacement keeps only the join condition, so any
            // residual conjunct (left- or right-referencing) would be lost.
            size_t null_checks = 0;
            bool convertible = true;
            for (const Expression& conjunct :
                 SplitConjuncts(*expression.predicate)) {
              if (!conjunct || conjunct->Type() != TypeTag::kUnaryExp ||
                  conjunct->AsUnaryExpression().Op() !=
                      UnaryOperation::kIsNull) {
                convertible = false;
                break;
              }
              const Expression& child = conjunct->AsUnaryExpression().Child();
              if (!child || child->Type() != TypeTag::kColumnValue) {
                convertible = false;
                break;
              }
              const ColumnName& col = child->AsColumnValue().GetColumnName();
              if (!resolves_to_right(col)) {
                convertible = false;
                break;
              }
              // A matched row whose null-tested column is genuinely NULL
              // satisfies `col IS NULL` but is dropped by the AntiJoin;
              // the rewrite is only sound for NOT NULL columns.
              if (!ColumnIsDeclaredNonNull(memo, col.schema, col)) {
                convertible = false;
                break;
              }
              ++null_checks;
            }
            if (convertible && null_checks > 0) {
              memo.AddExpression(
                  group, LogicalExpression{
                             .operation = LogicalOperator::kAntiJoin,
                             .children = {left_id, right_id},
                             .predicate = join_expr.predicate,
                             .target_list = expression.target_list.empty()
                                                ? join_expr.target_list
                                                : expression.target_list,
                             .output_schema = expression.output_schema});
            }
          }
        },
        LogicalOperator::kSelection));

    // right_to_left_outer_join: Normalize RightOuterJoin(L, R, p) ->
    // LeftOuterJoin(R, L, p).
    built.Add(Rule(
        "right_to_left_outer_join", OuterJoin(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kOuterJoin ||
              expression.children.size() != 2) {
            return;
          }
          if (expression.join_type == 1) {  // 1 = RightOuter
            const GroupId left_id = bindings.at("left");
            const GroupId right_id = bindings.at("right");
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {right_id, left_id},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .join_type = 0,  // 0 = LeftOuter
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kOuterJoin));

    // full_outer_join_decomposition: Decompose FullOuterJoin(L, R) into
    // UnionAll(LeftOuter(L, R), AntiJoin(R, L)) alternative.
    built.Add(Rule(
        "full_outer_join_decomposition", OuterJoin(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kOuterJoin ||
              expression.children.size() != 2) {
            return;
          }
          if (expression.join_type == 2) {  // 2 = FullOuter
            const GroupId left_id = bindings.at("left");
            const GroupId right_id = bindings.at("right");
            const auto schema_for = [&memo](GroupId child) {
              for (const LogicalExpression& child_expression :
                   memo.Get(child).expressions) {
                if (child_expression.output_schema.ColumnCount() != 0) {
                  return child_expression.output_schema;
                }
              }
              const std::vector<std::string>& relations =
                  memo.Get(child).relations;
              if (relations.size() != 1) {
                return Schema();
              }
              const auto found = memo.GetTableSchemas().find(relations.front());
              return found == memo.GetTableSchemas().end() ? Schema()
                                                           : found->second;
            };
            const auto qualify_singleton = [&memo](GroupId child,
                                                   Schema schema) {
              const std::vector<std::string>& relations =
                  memo.Get(child).relations;
              if (relations.size() != 1) {
                return schema;
              }
              std::vector<Column> columns;
              columns.reserve(schema.ColumnCount());
              for (size_t i = 0; i < schema.ColumnCount(); ++i) {
                Column column = schema.GetColumn(i);
                column.Name() =
                    ColumnName(relations.front(), column.Name().name);
                columns.push_back(std::move(column));
              }
              return Schema("", std::move(columns));
            };
            const Schema left_schema =
                qualify_singleton(left_id, schema_for(left_id));
            const Schema right_schema =
                qualify_singleton(right_id, schema_for(right_id));
            if (left_schema.ColumnCount() == 0 ||
                right_schema.ColumnCount() == 0) {
              return;
            }
            const Schema full_schema = left_schema + right_schema;
            std::vector<NamedExpression> padded_targets;
            padded_targets.reserve(static_cast<size_t>(
                left_schema.ColumnCount() + right_schema.ColumnCount()));
            for (size_t i = 0; i < left_schema.ColumnCount(); ++i) {
              padded_targets.emplace_back(left_schema.GetColumn(i).Name().name,
                                          ConstantValueExp(Value()));
            }
            for (size_t i = 0; i < right_schema.ColumnCount(); ++i) {
              const ColumnName column = right_schema.GetColumn(i).Name();
              padded_targets.emplace_back(column.name, ColumnValueExp(column));
            }
            const std::string predicate_key =
                expression.predicate ? (*expression.predicate)->ToString()
                                     : std::string("<null>");
            const std::string child_key =
                std::to_string(left_id) + ":" + std::to_string(right_id);
            const std::vector<std::string> cur_relations =
                memo.Get(group).relations;
            const GroupId left_outer_group = memo.EnsureDerivedGroup(
                cur_relations,
                "full_to_left_outer:" + child_key + ":" + predicate_key);
            const GroupId anti_join_group = memo.EnsureDerivedGroup(
                cur_relations,
                "full_to_anti_join:" + child_key + ":" + predicate_key);
            const GroupId padded_group = memo.EnsureDerivedGroup(
                cur_relations,
                "full_to_padded_anti:" + child_key + ":" + predicate_key);
            if (left_outer_group == group || anti_join_group == group ||
                padded_group == group) {
              return;
            }
            memo.AddExpression(
                left_outer_group,
                LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {left_id, right_id},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .join_type = 0,
                                  .output_schema = full_schema});

            memo.AddExpression(
                anti_join_group,
                LogicalExpression{.operation = LogicalOperator::kAntiJoin,
                                  .children = {right_id, left_id},
                                  .predicate = expression.predicate,
                                  .output_schema = right_schema});

            memo.AddExpression(
                padded_group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = {anti_join_group},
                                  .target_list = std::move(padded_targets),
                                  .output_schema = full_schema});

            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                  .children = {left_outer_group, padded_group},
                                  .target_list = expression.target_list,
                                  .output_schema = full_schema});
          }
        },
        LogicalOperator::kOuterJoin));

    // outer_to_inner_join_on_null_rejecting_filter: When Selection over
    // OuterJoin has null-rejecting predicates on the outer-generated nullable
    // relations, rewrite to InnerJoin with the same join condition.
    built.Add(Rule(
        "outer_to_inner_join_on_null_rejecting_filter", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const Group& input_group = memo.Get(input_id);
          for (const LogicalExpression& join_expr : input_group.expressions) {
            if (join_expr.operation != LogicalOperator::kOuterJoin ||
                join_expr.children.size() != 2) {
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            const auto& left_rels = memo.Get(left_id).relations;
            const auto& right_rels = memo.Get(right_id).relations;

            bool can_convert = false;
            if (join_expr.join_type == 0) {  // LeftOuter
              can_convert = ExpressionRejectsNullsOnRelations(
                  *expression.predicate, right_rels);
            } else if (join_expr.join_type == 1) {  // RightOuter
              can_convert = ExpressionRejectsNullsOnRelations(
                  *expression.predicate, left_rels);
            } else if (join_expr.join_type == 2) {  // FullOuter
              can_convert = ExpressionRejectsNullsOnRelations(
                                *expression.predicate, left_rels) &&
                            ExpressionRejectsNullsOnRelations(
                                *expression.predicate, right_rels);
            }
            if (!can_convert) {
              continue;
            }

            const GroupId inner_group = memo.EnsureDerivedGroup(
                memo.Get(input_id).relations,
                "outer_to_inner:" + GroupIdsTag({left_id, right_id}) +
                    PredicateTag(join_expr.predicate));
            if (inner_group == group || inner_group == input_id) {
              continue;
            }
            memo.AddExpression(
                inner_group,
                LogicalExpression{.operation = LogicalOperator::kJoin,
                                  .children = {left_id, right_id},
                                  .predicate = join_expr.predicate,
                                  .target_list = join_expr.target_list,
                                  .output_schema = join_expr.output_schema});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {inner_group},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kSelection));

    // outer_join_associativity: (A LEFT JOIN B ON p1) LEFT JOIN C ON p2 ->
    // A LEFT JOIN (B LEFT JOIN C ON p2) ON p1 when p2 rejects nulls on B.
    built.Add(Rule(
        "outer_join_associativity", OuterJoin(Any("input"), Any("c")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kOuterJoin ||
              expression.children.size() != 2 || expression.join_type != 0 ||
              !expression.predicate || !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const GroupId c_id = bindings.at("c");
          const Group& input_group = memo.Get(input_id);
          for (const LogicalExpression& inner_oj : input_group.expressions) {
            if (inner_oj.operation != LogicalOperator::kOuterJoin ||
                inner_oj.children.size() != 2 || inner_oj.join_type != 0) {
              continue;
            }
            const GroupId a_id = inner_oj.children[0];
            const GroupId b_id = inner_oj.children[1];
            const auto& b_rels = memo.Get(b_id).relations;
            const auto& c_rels = memo.Get(c_id).relations;

            bool touches_only_bc = true;
            for (const auto& col : (*expression.predicate)->TouchedColumns()) {
              if (std::ranges::find(b_rels, col.schema) == b_rels.end() &&
                  std::ranges::find(c_rels, col.schema) == c_rels.end()) {
                touches_only_bc = false;
                break;
              }
            }
            if (!touches_only_bc) {
              continue;
            }
            if (!ExpressionRejectsNullsOnRelations(*expression.predicate,
                                                   b_rels)) {
              continue;
            }

            const GroupId bc_group = memo.EnsureDerivedGroup(
                UnionRelations(b_rels, c_rels),
                "oj_assoc_bc:" + GroupIdsTag({b_id, c_id}) +
                    PredicateTag(expression.predicate));
            if (bc_group == group || bc_group == b_id || bc_group == c_id) {
              continue;
            }
            memo.AddExpression(
                bc_group,
                LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {b_id, c_id},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .join_type = 0,
                                  .output_schema = expression.output_schema});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {a_id, bc_group},
                                  .predicate = inner_oj.predicate,
                                  .target_list = expression.target_list,
                                  .join_type = 0,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kOuterJoin));

    // semi_join_inner_join_reorder: (R SEMI JOIN S ON p1) JOIN T ON p2 ->
    // (R JOIN T ON p2) SEMI JOIN S ON p1.
    built.Add(Rule(
        "semi_join_inner_join_reorder", Join(Any("semi_child"), Any("t")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kJoin ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId semi_child_id = bindings.at("semi_child");
          const GroupId t_id = bindings.at("t");
          const Group& semi_child_group = memo.Get(semi_child_id);
          for (const LogicalExpression& semi_expr :
               semi_child_group.expressions) {
            if (semi_expr.operation != LogicalOperator::kSemiJoin ||
                semi_expr.children.size() != 2) {
              continue;
            }
            const GroupId r_id = semi_expr.children[0];
            const GroupId s_id = semi_expr.children[1];
            const auto& r_rels = memo.Get(r_id).relations;
            const auto& t_rels = memo.Get(t_id).relations;

            if (expression.predicate && *expression.predicate) {
              bool touches_only_rt = true;
              for (const auto& col :
                   (*expression.predicate)->TouchedColumns()) {
                if (std::ranges::find(r_rels, col.schema) == r_rels.end() &&
                    std::ranges::find(t_rels, col.schema) == t_rels.end()) {
                  touches_only_rt = false;
                  break;
                }
              }
              if (!touches_only_rt) {
                continue;
              }
            }

            const GroupId rt_group = memo.EnsureDerivedGroup(
                UnionRelations(r_rels, t_rels),
                "semi_inner_reorder_rt:" + GroupIdsTag({r_id, t_id}) +
                    PredicateTag(expression.predicate));
            if (rt_group == group || rt_group == r_id || rt_group == t_id) {
              continue;
            }
            memo.AddExpression(
                rt_group,
                LogicalExpression{.operation = LogicalOperator::kJoin,
                                  .children = {r_id, t_id},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                  .children = {rt_group, s_id},
                                  .predicate = semi_expr.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kJoin));

    // semi_join_commutativity: (R SEMI JOIN S ON p1) SEMI JOIN T ON p2 ->
    // (R SEMI JOIN T ON p2) SEMI JOIN S ON p1.
    built.Add(Rule(
        "semi_join_commutativity", SemiJoin(Any("inner_semi"), Any("t")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSemiJoin ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId inner_semi_id = bindings.at("inner_semi");
          const GroupId t_id = bindings.at("t");
          const Group& inner_semi_group = memo.Get(inner_semi_id);
          for (const LogicalExpression& semi_expr :
               inner_semi_group.expressions) {
            if (semi_expr.operation != LogicalOperator::kSemiJoin ||
                semi_expr.children.size() != 2) {
              continue;
            }
            const GroupId r_id = semi_expr.children[0];
            const GroupId s_id = semi_expr.children[1];
            const auto& r_rels = memo.Get(r_id).relations;
            const auto& t_rels = memo.Get(t_id).relations;

            if (expression.predicate && *expression.predicate) {
              bool touches_only_rt = true;
              for (const auto& col :
                   (*expression.predicate)->TouchedColumns()) {
                if (std::ranges::find(r_rels, col.schema) == r_rels.end() &&
                    std::ranges::find(t_rels, col.schema) == t_rels.end()) {
                  touches_only_rt = false;
                  break;
                }
              }
              if (!touches_only_rt) {
                continue;
              }
            }

            const GroupId rt_group = memo.EnsureDerivedGroup(
                UnionRelations(r_rels, t_rels),
                "semi_comm_rt:" + GroupIdsTag({r_id, t_id}) +
                    PredicateTag(expression.predicate));
            if (rt_group == group || rt_group == r_id || rt_group == t_id) {
              continue;
            }
            memo.AddExpression(
                rt_group,
                LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                  .children = {r_id, t_id},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                  .children = {rt_group, s_id},
                                  .predicate = semi_expr.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kSemiJoin));

    // push_down_limit_through_join is DISABLED (see the disabled-rules table
    // in docs/cascades_optimizer.md): a uniqueness proof on the right join
    // keys bounds the match count at one but does not prove every left row
    // matches, so LIMIT pushed into the left side truncates the join's own
    // output whenever an unmatched left row sits inside the limit prefix.

    // push_selection_through_window: Push Selection conjuncts that reference
    // only PARTITION BY columns below the Window operator.
    built.Add(Rule(
        "push_selection_through_window", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const Group& input_group = memo.Get(input_id);
          for (const LogicalExpression& win_expr : input_group.expressions) {
            if (win_expr.operation != LogicalOperator::kWindow ||
                win_expr.children.size() != 1 ||
                win_expr.partition_by.empty()) {
              continue;
            }
            std::vector<ColumnName> partition_cols;
            for (const auto& p : win_expr.partition_by) {
              if (p && p->Type() == TypeTag::kColumnValue) {
                partition_cols.push_back(p->AsColumnValue().GetColumnName());
              }
            }
            if (partition_cols.empty()) {
              continue;
            }

            std::unordered_set<std::string> window_outputs;
            for (const NamedExpression& target : win_expr.target_list) {
              window_outputs.insert(target.name);
            }

            std::vector<Expression> pushable;
            std::vector<Expression> remaining;
            for (const auto& conjunct : SplitConjuncts(*expression.predicate)) {
              if (!conjunct) {
                continue;
              }
              bool can_push = true;
              auto touched = conjunct->TouchedColumns();
              if (touched.empty()) {
                can_push = false;
              }
              for (const auto& col : touched) {
                const bool in_partition = std::ranges::any_of(
                    partition_cols, [&](const ColumnName& partition) {
                      return SameColumnReference(col, partition);
                    });
                if (window_outputs.contains(col.name) || !in_partition) {
                  can_push = false;
                  break;
                }
              }
              if (can_push) {
                pushable.push_back(conjunct);
              } else {
                remaining.push_back(conjunct);
              }
            }
            if (pushable.empty()) {
              continue;
            }

            const GroupId win_child = win_expr.children[0];
            const GroupId win_child_filtered = memo.EnsureDerivedGroup(
                memo.Get(win_child).relations,
                "win_part_push:" + GroupIdsTag({win_child}) +
                    CombineConjuncts(pushable)->ToString());
            InheritBranchLocal(memo, win_child_filtered, group);
            if (win_child_filtered == win_child ||
                win_child_filtered == group) {
              continue;
            }
            memo.AddExpression(
                win_child_filtered,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {win_child},
                                  .predicate = CombineConjuncts(pushable)});

            if (remaining.empty()) {
              LogicalExpression new_win = win_expr;
              new_win.children = {win_child_filtered};
              memo.AddExpression(group, std::move(new_win));
            } else {
              const GroupId new_win_group = memo.EnsureDerivedGroup(
                  memo.Get(input_id).relations,
                  "win_pushed:" + GroupIdsTag({win_child_filtered}) +
                      TargetListFingerprint(win_expr.target_list));
              if (new_win_group == group || new_win_group == input_id) {
                continue;
              }
              LogicalExpression new_win = win_expr;
              new_win.children = {win_child_filtered};
              memo.AddExpression(new_win_group, std::move(new_win));

              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {new_win_group},
                                    .predicate = CombineConjuncts(remaining)});
            }
          }
        },
        LogicalOperator::kSelection));

    // infer_filter_from_equivalence_class: Given an equality condition between
    // tables (an equivalence class {T1.c, T2.d}) and a filter on T1.c (e.g.
    // T1.c OP Const), infer T2.d OP Const and push it into the selection group.
    built.Add(Rule(
        "infer_filter_from_equivalence_class", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const auto& eq_classes =
              memo.Get(input_id).logical_properties.equivalence_classes;
          if (eq_classes.empty()) {
            return;
          }

          std::vector<Expression> existing_conjuncts =
              SplitConjuncts(*expression.predicate);
          std::vector<Expression> new_inferred;

          // The recorded classes are pairs reported by individual join
          // expressions; union-find merges them so a filter can infer across
          // an a=b, b=c chain and not only direct pairs. Every member of a
          // merged class is equal on every row the inner joins emit, so
          // substituting the filter column by any member keeps the selection
          // semantically unchanged (NULL rows never satisfy a join equality).
          std::unordered_map<std::string, size_t> member_node;
          std::vector<size_t> class_parent;
          const auto find_root = [&class_parent](size_t node) {
            while (class_parent[node] != node) {
              class_parent[node] = class_parent[class_parent[node]];
              node = class_parent[node];
            }
            return node;
          };
          const auto node_for = [&](const std::string& member) {
            const auto [it, inserted] =
                member_node.emplace(member, class_parent.size());
            if (inserted) {
              class_parent.push_back(class_parent.size());
            }
            return it->second;
          };
          for (const auto& ec : eq_classes) {
            if (ec.size() < 2) {
              continue;
            }
            auto member = ec.begin();
            const size_t first = node_for(*member);
            for (++member; member != ec.end(); ++member) {
              const size_t left = find_root(first);
              const size_t right = find_root(node_for(*member));
              if (left != right) {
                class_parent[left] = right;
              }
            }
          }

          std::unordered_set<std::string> known_texts;
          for (const auto& existing : existing_conjuncts) {
            if (existing) {
              known_texts.insert(existing->ToString());
            }
          }
          std::unordered_set<std::string> inferred_texts;
          for (const auto& conjunct : existing_conjuncts) {
            if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
              continue;
            }
            const auto& bin = conjunct->AsBinaryExpression();
            const auto op = bin.Op();
            if (op != BinaryOperation::kEquals &&
                op != BinaryOperation::kNotEquals &&
                op != BinaryOperation::kGreaterThan &&
                op != BinaryOperation::kGreaterThanEquals &&
                op != BinaryOperation::kLessThan &&
                op != BinaryOperation::kLessThanEquals) {
              continue;
            }
            const Expression* col_expr = nullptr;
            const Expression* const_expr = nullptr;
            bool col_on_left = true;
            if (bin.Left()->Type() == TypeTag::kColumnValue &&
                bin.Right()->Type() == TypeTag::kConstantValue) {
              col_expr = &bin.Left();
              const_expr = &bin.Right();
            } else if (bin.Right()->Type() == TypeTag::kColumnValue &&
                       bin.Left()->Type() == TypeTag::kConstantValue) {
              col_expr = &bin.Right();
              const_expr = &bin.Left();
              col_on_left = false;
            }
            if (!col_expr || !const_expr) {
              continue;
            }

            const auto& col_name = (*col_expr)->AsColumnValue().GetColumnName();
            const std::string col_str = col_name.ToString();
            const auto found = member_node.find(col_str);
            if (found == member_node.end()) {
              continue;
            }
            const size_t root = find_root(found->second);
            for (const auto& [member, node] : member_node) {
              if (member == col_str || find_root(node) != root) {
                continue;
              }
              Expression inferred_bin =
                  col_on_left
                      ? BinaryExpressionExp(ColumnValueExp(ColumnName(member)),
                                            op, *const_expr)
                      : BinaryExpressionExp(*const_expr, op,
                                            ColumnValueExp(ColumnName(member)));
              if (known_texts.insert(inferred_bin->ToString()).second &&
                  inferred_texts.insert(inferred_bin->ToString()).second) {
                new_inferred.push_back(std::move(inferred_bin));
              }
            }
          }

          if (!new_inferred.empty()) {
            std::vector<Expression> combined = existing_conjuncts;
            for (const auto& inf : new_inferred) {
              combined.push_back(inf);
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {input_id},
                                  .predicate = CombineConjuncts(combined)});
          }
        },
        LogicalOperator::kSelection));

    // push_semi_join_through_inner_join: (A JOIN B ON p_ab) SEMI JOIN C ON
    // p_semi -> (A SEMI JOIN C ON p_semi) JOIN B ON p_ab (if p_semi touches
    // only A and C) or A JOIN (B SEMI JOIN C ON p_semi) ON p_ab (if p_semi
    // touches only B and C).
    built.Add(Rule(
        "push_semi_join_through_inner_join",
        SemiJoin(Any("inner_join"), Any("c")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSemiJoin ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId inner_join_id = bindings.at("inner_join");
          const GroupId c_id = bindings.at("c");
          const std::vector<std::string> c_rels = memo.Get(c_id).relations;
          const std::vector<LogicalExpression> join_expressions =
              memo.Get(inner_join_id).expressions;

          for (const LogicalExpression& join_expr : join_expressions) {
            if (join_expr.operation != LogicalOperator::kJoin ||
                join_expr.children.size() != 2) {
              continue;
            }
            const GroupId a_id = join_expr.children[0];
            const GroupId b_id = join_expr.children[1];
            const std::vector<std::string> a_rels = memo.Get(a_id).relations;
            const std::vector<std::string> b_rels = memo.Get(b_id).relations;

            if (!expression.predicate || !*expression.predicate) {
              continue;
            }

            bool touches_b = false;
            bool touches_a = false;
            bool touches_only_valid = true;
            for (const auto& col : (*expression.predicate)->TouchedColumns()) {
              bool in_a = std::ranges::find(a_rels, col.schema) != a_rels.end();
              bool in_b = std::ranges::find(b_rels, col.schema) != b_rels.end();
              bool in_c = std::ranges::find(c_rels, col.schema) != c_rels.end();
              if (in_a) {
                touches_a = true;
              }
              if (in_b) {
                touches_b = true;
              }
              if (!in_a && !in_b && !in_c) {
                touches_only_valid = false;
                break;
              }
            }
            if (!touches_only_valid) {
              continue;
            }

            if (touches_a && !touches_b) {
              const GroupId ac_group = memo.EnsureDerivedGroup(
                  UnionRelations(a_rels, c_rels),
                  "semi_inner_push_ac:" + GroupIdsTag({a_id, c_id}) +
                      PredicateTag(expression.predicate));
              if (ac_group == group || ac_group == a_id || ac_group == c_id) {
                continue;
              }
              memo.AddExpression(
                  ac_group,
                  LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                    .children = {a_id, c_id},
                                    .predicate = expression.predicate,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {ac_group, b_id},
                                    .predicate = join_expr.predicate,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            } else if (touches_b && !touches_a) {
              const GroupId bc_group = memo.EnsureDerivedGroup(
                  UnionRelations(b_rels, c_rels),
                  "semi_inner_push_bc:" + GroupIdsTag({b_id, c_id}) +
                      PredicateTag(expression.predicate));
              if (bc_group == group || bc_group == b_id || bc_group == c_id) {
                continue;
              }
              memo.AddExpression(
                  bc_group,
                  LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                    .children = {b_id, c_id},
                                    .predicate = expression.predicate,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {a_id, bc_group},
                                    .predicate = join_expr.predicate,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            }
          }
        },
        LogicalOperator::kSemiJoin));

    // push_limit_through_left_join: Limit over LeftOuterJoin where right join
    // keys form a candidate key on the right side.
    built.Add(Rule(
        "push_limit_through_left_join", Limit(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kLimit ||
              expression.children.size() != 1) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const Group& input_group = memo.Get(input_id);
          if (expression.limit_offset >
              std::numeric_limits<size_t>::max() - expression.limit_count) {
            return;
          }
          const size_t total_limit =
              expression.limit_count + expression.limit_offset;
          if (total_limit == 0) {
            return;
          }

          for (const LogicalExpression& ojoin : input_group.expressions) {
            if (ojoin.operation != LogicalOperator::kOuterJoin ||
                ojoin.join_type != 0 || ojoin.children.size() != 2 ||
                !ojoin.predicate.has_value() || !*ojoin.predicate ||
                !SafeToReduceEvaluationCount(*ojoin.predicate) ||
                !ExpressionCannotThrowForPlanner(*ojoin.predicate)) {
              continue;
            }
            const GroupId left_id = ojoin.children[0];
            const GroupId right_id = ojoin.children[1];

            bool left_already_limited = false;
            for (const auto& lexpr : memo.Get(left_id).expressions) {
              if (lexpr.operation == LogicalOperator::kLimit &&
                  lexpr.limit_count <= total_limit) {
                left_already_limited = true;
                break;
              }
            }
            if (left_already_limited) {
              continue;
            }

            std::unordered_set<std::string> right_join_cols;
            const auto& right_rels = memo.Get(right_id).relations;
            for (const auto& conjunct : SplitConjuncts(*ojoin.predicate)) {
              if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
                continue;
              }
              const auto& bin = conjunct->AsBinaryExpression();
              if (bin.Op() != BinaryOperation::kEquals) {
                continue;
              }
              if (bin.Left()->Type() == TypeTag::kColumnValue) {
                const auto& col = bin.Left()->AsColumnValue().GetColumnName();
                if (std::ranges::find(right_rels, col.schema) !=
                    right_rels.end()) {
                  right_join_cols.insert(col.ToString());
                  right_join_cols.insert(col.name);
                }
              }
              if (bin.Right()->Type() == TypeTag::kColumnValue) {
                const auto& col = bin.Right()->AsColumnValue().GetColumnName();
                if (std::ranges::find(right_rels, col.schema) !=
                    right_rels.end()) {
                  right_join_cols.insert(col.ToString());
                  right_join_cols.insert(col.name);
                }
              }
            }
            if (right_join_cols.empty() ||
                !memo.Get(right_id).logical_properties.IsUniqueOn(
                    right_join_cols) ||
                memo.Get(left_id).relations.size() != 1 ||
                memo.Get(right_id).relations.size() != 1 ||
                !memo.IsProvenOneToOne(memo.Get(left_id).relations.front(),
                                       memo.Get(right_id).relations.front())) {
              continue;
            }
            bool exact_key_conjunction = true;
            for (const Expression& conjunct :
                 SplitConjuncts(*ojoin.predicate)) {
              if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
                exact_key_conjunction = false;
                break;
              }
              const auto& binary = conjunct->AsBinaryExpression();
              if (binary.Op() != BinaryOperation::kEquals ||
                  binary.Left()->Type() != TypeTag::kColumnValue ||
                  binary.Right()->Type() != TypeTag::kColumnValue) {
                exact_key_conjunction = false;
                break;
              }
              const ColumnName& lhs =
                  binary.Left()->AsColumnValue().GetColumnName();
              const ColumnName& rhs =
                  binary.Right()->AsColumnValue().GetColumnName();
              const bool lhs_left =
                  lhs.schema == memo.Get(left_id).relations.front();
              const bool rhs_right =
                  rhs.schema == memo.Get(right_id).relations.front();
              const bool lhs_right =
                  lhs.schema == memo.Get(right_id).relations.front();
              const bool rhs_left =
                  rhs.schema == memo.Get(left_id).relations.front();
              if (!((lhs_left && rhs_right) || (lhs_right && rhs_left))) {
                exact_key_conjunction = false;
                break;
              }
            }
            if (!exact_key_conjunction) {
              continue;
            }

            const std::string limit_tag =
                "ojoin_limit_left:" + std::to_string(total_limit) + ":" +
                std::to_string(left_id) + ":" + std::to_string(right_id) + ":" +
                (*ojoin.predicate)->ToString();
            const GroupId limit_left_group =
                memo.EnsureDerivedGroup(memo.Get(left_id).relations, limit_tag);
            if (limit_left_group == left_id || limit_left_group == group) {
              continue;
            }
            memo.AddExpression(
                limit_left_group,
                LogicalExpression{.operation = LogicalOperator::kLimit,
                                  .children = {left_id},
                                  .limit_count = total_limit,
                                  .limit_offset = 0});

            const GroupId new_ojoin_group = memo.EnsureDerivedGroup(
                memo.Get(input_id).relations,
                "ojoin_limited_left:" + std::to_string(total_limit) + ":" +
                    std::to_string(left_id) + ":" + std::to_string(right_id) +
                    ":" + (*ojoin.predicate)->ToString());
            if (new_ojoin_group == group || new_ojoin_group == input_id) {
              continue;
            }
            LogicalExpression new_ojoin = ojoin;
            new_ojoin.children = {limit_left_group, right_id};
            memo.AddExpression(new_ojoin_group, std::move(new_ojoin));

            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kLimit,
                                  .children = {new_ojoin_group},
                                  .limit_count = expression.limit_count,
                                  .limit_offset = expression.limit_offset});
          }
        },
        LogicalOperator::kLimit));

    // merge_adjacent_windows: Fuse Window_1(Window_2(X)) into a single
    // Window(X) when both windows have identical PARTITION BY and Window_1
    // does not reference Window_2's outputs. ORDER BY/frame specifications
    // remain on each target's WindowFunctionCallExpression.
    built.Add(Rule(
        "merge_adjacent_windows", Window(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kWindow ||
              expression.children.size() != 1) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const Group& input_group = memo.Get(input_id);

          for (const LogicalExpression& inner_win : input_group.expressions) {
            if (inner_win.operation != LogicalOperator::kWindow ||
                inner_win.children.size() != 1 ||
                inner_win.children[0] == group) {
              continue;
            }
            if (expression.partition_by.size() !=
                inner_win.partition_by.size()) {
              continue;
            }
            bool part_match = true;
            for (size_t i = 0; i < expression.partition_by.size(); ++i) {
              if (expression.partition_by[i]->ToString() !=
                  inner_win.partition_by[i]->ToString()) {
                part_match = false;
                break;
              }
            }
            if (!part_match) {
              continue;
            }

            // Normal window lowering leaves these node-level sort flags
            // empty; keep a conservative guard for explicitly supplied IR.
            if (expression.sort_ascending != inner_win.sort_ascending ||
                expression.sort_nulls_first != inner_win.sort_nulls_first) {
              continue;
            }

            std::unordered_set<std::string> inner_outputs;
            for (const auto& target : inner_win.target_list) {
              inner_outputs.insert(target.name);
            }
            bool touches_inner_output = false;
            for (const auto& target : expression.target_list) {
              if (!target.expression) {
                continue;
              }
              for (const auto& col : target.expression->TouchedColumns()) {
                if (inner_outputs.contains(col.name)) {
                  touches_inner_output = true;
                  break;
                }
              }
              if (touches_inner_output) {
                break;
              }
            }
            if (touches_inner_output) {
              continue;
            }

            std::vector<NamedExpression> merged_targets = inner_win.target_list;
            for (const auto& target : expression.target_list) {
              merged_targets.push_back(target);
            }

            LogicalExpression merged_win = expression;
            merged_win.children = inner_win.children;
            merged_win.target_list = std::move(merged_targets);
            memo.AddExpression(group, std::move(merged_win));
          }
        },
        LogicalOperator::kWindow));

    // push_not_through_expression: Push NOT operators through comparisons,
    // double negations, and De Morgan's laws (AND / OR) into canonical form.
    built.Add(Rule(
        "push_not_through_expression", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          (void)bindings;
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const auto normalize_not = [](auto& self,
                                        const Expression& expr) -> Expression {
            if (!expr) {
              return expr;
            }
            if (expr->Type() == TypeTag::kUnaryExp) {
              const auto& un = expr->AsUnaryExpression();
              if (un.Op() == UnaryOperation::kNot && un.Child()) {
                const auto& child = un.Child();
                if (child->Type() == TypeTag::kUnaryExp) {
                  const auto& inner_un = child->AsUnaryExpression();
                  if (inner_un.Op() == UnaryOperation::kNot &&
                      inner_un.Child()) {
                    return self(self, inner_un.Child());
                  }
                } else if (child->Type() == TypeTag::kBinaryExp) {
                  const auto& bin = child->AsBinaryExpression();
                  const auto op = bin.Op();
                  const auto& l = bin.Left();
                  const auto& r = bin.Right();
                  switch (op) {
                    case BinaryOperation::kEquals:
                      return BinaryExpressionExp(self(self, l),
                                                 BinaryOperation::kNotEquals,
                                                 self(self, r));
                    case BinaryOperation::kNotEquals:
                      return BinaryExpressionExp(self(self, l),
                                                 BinaryOperation::kEquals,
                                                 self(self, r));
                    case BinaryOperation::kAnd:
                      return BinaryExpressionExp(
                          self(self,
                               UnaryExpressionExp(l, UnaryOperation::kNot)),
                          BinaryOperation::kOr,
                          self(self,
                               UnaryExpressionExp(r, UnaryOperation::kNot)));
                    case BinaryOperation::kOr:
                      return BinaryExpressionExp(
                          self(self,
                               UnaryExpressionExp(l, UnaryOperation::kNot)),
                          BinaryOperation::kAnd,
                          self(self,
                               UnaryExpressionExp(r, UnaryOperation::kNot)));
                    default:
                      break;
                  }
                }
              }
            } else if (expr->Type() == TypeTag::kBinaryExp) {
              const auto& bin = expr->AsBinaryExpression();
              auto l_norm = self(self, bin.Left());
              auto r_norm = self(self, bin.Right());
              if (l_norm->ToString() != bin.Left()->ToString() ||
                  r_norm->ToString() != bin.Right()->ToString()) {
                return BinaryExpressionExp(l_norm, bin.Op(), r_norm);
              }
            }
            return expr;
          };

          Expression normalized =
              normalize_not(normalize_not, *expression.predicate);
          if (normalized &&
              normalized->ToString() != (*expression.predicate)->ToString()) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = expression.children,
                                  .predicate = normalized});
          }
        },
        LogicalOperator::kSelection));

    // no_op_window_elimination: Remove Window operator when the outer
    // expressions do not reference any window function results. Both
    // bypasses below are gated on exactly that: dropping a Window node
    // whose `$win` outputs (or raw calls) are still referenced would
    // orphan columns (or evaluate calls row-at-a-time, which the
    // evaluator rejects).
    built.Add(Rule(
        "no_op_window_elimination", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation == LogicalOperator::kWindow &&
              expression.children.size() == 1) {
            const GroupId child = expression.children[0];
            if (child != group) {
              if (expression.target_list.empty()) {
                for (const LogicalExpression& child_expr :
                     memo.Get(child).expressions) {
                  bool refs_group = false;
                  for (GroupId c : child_expr.children) {
                    if (c == group) {
                      refs_group = true;
                      break;
                    }
                  }
                  if (refs_group) {
                    continue;
                  }
                  memo.AddExpression(group, child_expr);
                }
              } else if (!std::ranges::any_of(
                             expression.target_list,
                             [](const NamedExpression& target) {
                               return target.expression &&
                                      TreeContainsWindowCall(target.expression);
                             })) {
                memo.AddExpression(
                    group, LogicalExpression{
                               .operation = LogicalOperator::kProjection,
                               .children = {child},
                               .target_list = expression.target_list,
                               .output_schema = expression.output_schema});
              }
            }
          } else if (expression.operation == LogicalOperator::kProjection &&
                     expression.children.size() == 1) {
            const GroupId in_id = expression.children[0];
            if (in_id != group) {
              // The outer targets must not reference the window outputs:
              // `$winN` references are emitted bare, so a bare name match
              // against the window's output names blocks the bypass (as
              // does any raw window call, which needs whole-partition
              // context to evaluate).
              const auto outer_uses_window = [](const LogicalExpression& outer,
                                                const LogicalExpression& win) {
                std::unordered_set<std::string> outputs;
                for (const NamedExpression& target : win.target_list) {
                  if (!target.name.empty()) {
                    outputs.insert(target.name);
                  }
                }
                for (const NamedExpression& target : outer.target_list) {
                  if (!target.expression) {
                    continue;
                  }
                  if (TreeContainsWindowCall(target.expression)) {
                    return true;
                  }
                  for (const ColumnName& column :
                       target.expression->TouchedColumns()) {
                    if (column.schema.empty() &&
                        outputs.contains(column.name)) {
                      return true;
                    }
                  }
                }
                return false;
              };
              const Group& input_group = memo.Get(in_id);
              for (const auto& win : input_group.expressions) {
                if (win.operation != LogicalOperator::kWindow ||
                    win.children.size() != 1 || win.children[0] == group) {
                  continue;
                }
                if (outer_uses_window(expression, win)) {
                  continue;
                }
                const GroupId child = win.children[0];
                memo.AddExpression(
                    group, LogicalExpression{
                               .operation = LogicalOperator::kProjection,
                               .children = {child},
                               .target_list = expression.target_list,
                               .output_schema = expression.output_schema});
              }
            }
          }
        },
        {LogicalOperator::kWindow, LogicalOperator::kProjection}));

    // dynamic_filter_pushdown_join: annotate the probe-side join keys a
    // dynamic ("shared information passing") filter can be built over
    // (Phase 2 "Dynamic Filter Join / SIP / Bloom").
    //
    // The annotation is purely logical: it names the columns, never the filter
    // shape. Which side actually builds is a cost decision, and
    // join_commutativity supplies the mirrored expression, whose children[1]
    // is this join's children[0] -- so this one child position already yields
    // BOTH build/probe orientations to the implementation rules.
    //
    // The derived group keeps the original child's selection (D5: "残余述語を
    // 保持し、フィルタ消失を許さない") and its tag fingerprints the key set, so
    // two different key sets can never collide into one derived group.
    built.Add(Rule(
        "dynamic_filter_pushdown_join", Join(Any("build"), Any("probe")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (memo.Get(group).tag.find("bloom") != std::string::npos ||
              memo.Get(group).relations.size() != 2) {
            return;
          }
          if (!expression.predicate || !*expression.predicate) {
            return;
          }
          const GroupId probe_id = bindings.at("probe");
          if (probe_id == group) {
            return;
          }
          // Fixpoint guard: the probe already is a dynamic-filter probe
          // group (join commutativity re-presents the filtered join).
          if (memo.Get(probe_id).tag.starts_with("bloom_probe:")) {
            return;
          }
          const std::vector<std::string> probe_rels =
              memo.Get(probe_id).relations;
          if (probe_rels.size() != 1) {
            return;
          }
          std::vector<ColumnName> bloom_keys;
          for (const Expression& conjunct :
               SplitConjuncts(*expression.predicate)) {
            if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
              continue;
            }
            const auto& binary = conjunct->AsBinaryExpression();
            if (binary.Op() != BinaryOperation::kEquals ||
                binary.Left()->Type() != TypeTag::kColumnValue ||
                binary.Right()->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName left =
                binary.Left()->AsColumnValue().GetColumnName();
            const ColumnName right =
                binary.Right()->AsColumnValue().GetColumnName();
            const bool left_here =
                std::ranges::find(probe_rels, left.schema) != probe_rels.end();
            const bool right_here =
                std::ranges::find(probe_rels, right.schema) != probe_rels.end();
            if (left_here == right_here) {
              continue;  // both sides here (self-join) or neither side here
            }
            const ColumnName& key = left_here ? left : right;
            if (std::ranges::find(bloom_keys, key) == bloom_keys.end()) {
              bloom_keys.push_back(key);
            }
          }
          if (bloom_keys.empty()) {
            return;
          }
          // D5 gate: the derived probe group must keep the original probe
          // group's selection ("残余述語を保持し、フィルタ消失を許さない").
          // EnsureDerivedGroup creates an unfiltered group; carrying the base
          // filter over makes the alternative predicate-equivalent to the
          // original probe child.
          const Expression probe_filter = memo.Get(probe_id).filter;
          std::string key_fingerprint;
          for (const ColumnName& key : bloom_keys) {
            key_fingerprint += key.ToString();
            key_fingerprint.push_back(',');
          }
          const GroupId filtered_probe = memo.EnsureDerivedGroup(
              probe_rels,
              "bloom_probe:" + GroupIdsTag({probe_id}) + key_fingerprint);
          if (filtered_probe != probe_id && filtered_probe != group) {
            memo.Get(filtered_probe).filter = probe_filter;
            // The logical half of the dynamic filter contract: these columns
            // hold the join key a build side publishes a sound membership /
            // range filter over. The executor picks the shape (exact set,
            // min/max, Bloom) from the build domain it observes at run time.
            memo.Get(filtered_probe).dynamic_filter_keys = bloom_keys;
            memo.AddExpression(
                filtered_probe,
                LogicalExpression{.operation = LogicalOperator::kScan,
                                  .table = probe_rels.front()});
            LogicalExpression rewritten = expression;
            rewritten.children[1] = filtered_probe;
            memo.AddExpression(group, std::move(rewritten));
          }
        },
        LogicalOperator::kJoin));

    // semi_reduction_pushdown_semi_join / semi_reduction_pushdown_anti_join:
    // annotate the probe-side join keys whose distinct values bound the build
    // side of a semi/anti join (Phase 2 "Hash Semi Reduction").
    //
    // A semi/anti join emits only the probe side, so its hash build is a pure
    // membership test over the join key. That makes two build-side reductions
    // sound: one row per distinct key, and only the keys the probe side
    // actually carries. These rules record the probe side's key columns -- the
    // input the domain is built from -- and nothing about the reduction
    // itself; the implementation rule decides whether the shape pays, and the
    // executor owns the domain, the dedup, and the fallback.
    //
    // The annotation lives on a derived group whose tag fingerprints the key
    // set, so a probe group that is the probe of two joins with DIFFERENT key
    // sets never has to merge two annotations into one vector. The derived
    // group keeps the probe group's selection (D5: "残余述語を保持し、フィルタ
    // 消失を許さない"), so the alternative is predicate-equivalent to the
    // original probe child.
    const auto semi_reduction_annotate =
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSemiJoin &&
              expression.operation != LogicalOperator::kAntiJoin) {
            return;
          }
          if (memo.Get(group).tag.find("semi_reduce") != std::string::npos ||
              memo.Get(group).relations.size() != 2) {
            return;
          }
          if (!expression.predicate || !*expression.predicate) {
            return;
          }
          const GroupId probe_id = bindings.at("probe");
          if (probe_id == group) {
            return;
          }
          // Fixpoint guard: the probe already is a semi-reduction probe.
          if (memo.Get(probe_id).tag.starts_with("semi_reduce_probe:")) {
            return;
          }
          const std::vector<std::string> probe_rels =
              memo.Get(probe_id).relations;
          if (probe_rels.empty()) {
            return;
          }
          std::vector<ColumnName> probe_keys;
          for (const Expression& conjunct :
               SplitConjuncts(*expression.predicate)) {
            if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
              continue;
            }
            const auto& binary = conjunct->AsBinaryExpression();
            if (binary.Op() != BinaryOperation::kEquals ||
                binary.Left()->Type() != TypeTag::kColumnValue ||
                binary.Right()->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName left =
                binary.Left()->AsColumnValue().GetColumnName();
            const ColumnName right =
                binary.Right()->AsColumnValue().GetColumnName();
            const bool left_here =
                std::ranges::find(probe_rels, left.schema) != probe_rels.end();
            const bool right_here =
                std::ranges::find(probe_rels, right.schema) != probe_rels.end();
            if (left_here == right_here) {
              continue;  // both sides here (self-join) or neither side here
            }
            const ColumnName& key = left_here ? left : right;
            if (std::ranges::find(probe_keys, key) == probe_keys.end()) {
              probe_keys.push_back(key);
            }
          }
          if (probe_keys.empty()) {
            return;
          }
          const Expression probe_filter = memo.Get(probe_id).filter;
          std::string key_fingerprint;
          for (const ColumnName& key : probe_keys) {
            key_fingerprint += key.ToString();
            key_fingerprint.push_back(',');
          }
          const GroupId keyed_probe = memo.EnsureDerivedGroup(
              probe_rels,
              "semi_reduce_probe:" + GroupIdsTag({probe_id}) + key_fingerprint);
          if (keyed_probe != probe_id && keyed_probe != group) {
            memo.Get(keyed_probe).filter = probe_filter;
            // The logical half of the semi-reduction contract: the executor
            // may build a distinct key domain over these columns and drop
            // build rows the probe side can never match.
            memo.Get(keyed_probe).semi_reduction_probe_keys = probe_keys;
            if (probe_rels.size() == 1) {
              memo.AddExpression(
                  keyed_probe,
                  LogicalExpression{.operation = LogicalOperator::kScan,
                                    .table = probe_rels.front()});
            }
            LogicalExpression rewritten = expression;
            rewritten.children[0] = keyed_probe;
            memo.AddExpression(group, std::move(rewritten));
          }
        };
    built.Add(Rule("semi_reduction_pushdown_semi_join",
                   SemiJoin(Any("probe"), Any("build")),
                   semi_reduction_annotate, LogicalOperator::kSemiJoin));
    built.Add(Rule("semi_reduction_pushdown_anti_join",
                   AntiJoin(Any("probe"), Any("build")),
                   semi_reduction_annotate, LogicalOperator::kAntiJoin));

    // join_predicate_transitivity: Infer non-constant equality transitive
    // chains across multi-way joins (t1.a = t2.b and t2.b = t3.c -> t1.a =
    // t3.c).
    built.Add(Rule(
        "join_predicate_transitivity", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate) {
            return;
          }
          const auto conjuncts = SplitConjuncts(*expression.predicate);
          std::vector<std::pair<ColumnName, ColumnName>> equalities;
          for (const auto& conj : conjuncts) {
            if (conj && conj->Type() == TypeTag::kBinaryExp) {
              const auto& bin = conj->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kEquals &&
                  bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kColumnValue) {
                equalities.emplace_back(
                    bin.Left()->AsColumnValue().GetColumnName(),
                    bin.Right()->AsColumnValue().GetColumnName());
              }
            }
          }
          if (equalities.size() < 2) {
            return;
          }
          std::vector<Expression> new_conjuncts = conjuncts;
          bool added = false;
          for (size_t i = 0; i < equalities.size(); ++i) {
            for (size_t j = 0; j < equalities.size(); ++j) {
              if (i == j) {
                continue;
              }
              const auto& [a1, b1] = equalities[i];
              const auto& [a2, b2] = equalities[j];
              std::optional<std::pair<ColumnName, ColumnName>> inferred;
              if (b1 == a2 && a1 != b2) {
                inferred = {a1, b2};
              } else if (b1 == b2 && a1 != a2) {
                inferred = {a1, a2};
              } else if (a1 == a2 && b1 != b2) {
                inferred = {b1, b2};
              } else if (a1 == b2 && b1 != a2) {
                inferred = {b1, a2};
              }

              if (inferred &&
                  inferred->first.schema != inferred->second.schema) {
                const Expression candidate = BinaryExpressionExp(
                    ColumnValueExp(inferred->first), BinaryOperation::kEquals,
                    ColumnValueExp(inferred->second));
                const std::string cand_str = candidate->ToString();
                const std::string rev_str =
                    BinaryExpressionExp(ColumnValueExp(inferred->second),
                                        BinaryOperation::kEquals,
                                        ColumnValueExp(inferred->first))
                        ->ToString();
                bool exists = false;
                for (const auto& c : new_conjuncts) {
                  if (c->ToString() == cand_str || c->ToString() == rev_str) {
                    exists = true;
                    break;
                  }
                }
                if (!exists) {
                  new_conjuncts.push_back(candidate);
                  added = true;
                }
              }
            }
          }
          if (added) {
            LogicalExpression rewritten = expression;
            rewritten.predicate =
                CanonicalizeConjuncts(CombineConjuncts(new_conjuncts));
            memo.AddExpression(group, std::move(rewritten));
          }
        },
        LogicalOperator::kJoin));

    // inferred_inequality_pushdown: Infer inequality predicates across
    // equalities (t1.x > 10 and t1.x = t2.y -> t2.y > 10).
    built.Add(Rule(
        "inferred_inequality_pushdown", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (SelectionMayStrengthenSharedScans(memo, group) &&
              expression.predicate) {
            InferJoinInequalities(memo, *expression.predicate);
          }
        },
        LogicalOperator::kJoin));

    // redundant_join_predicate_elimination: Eliminate redundant join conditions
    // already satisfied by transitivity.
    built.Add(Rule(
        "redundant_join_predicate_elimination", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate) {
            return;
          }
          const auto conjuncts = SplitConjuncts(*expression.predicate);
          if (conjuncts.size() < 3) {
            return;
          }
          std::unordered_map<std::string, std::string> parent;
          const auto find_root = [&](auto& self,
                                     const std::string& x) -> std::string {
            auto it = parent.find(x);
            if (it == parent.end() || it->second == x) {
              return x;
            }
            return it->second = self(self, it->second);
          };
          const auto unite = [&](const std::string& a,
                                 const std::string& b) -> bool {
            std::string root_a = find_root(find_root, a);
            std::string root_b = find_root(find_root, b);
            if (root_a == root_b) {
              return false;
            }
            parent[root_a] = root_b;
            return true;
          };

          std::vector<Expression> kept;
          bool removed = false;
          for (const auto& conj : conjuncts) {
            if (conj && conj->Type() == TypeTag::kBinaryExp) {
              const auto& bin = conj->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kEquals &&
                  bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kColumnValue) {
                const std::string u =
                    bin.Left()->AsColumnValue().GetColumnName().ToString();
                const std::string v =
                    bin.Right()->AsColumnValue().GetColumnName().ToString();
                if (unite(u, v)) {
                  kept.push_back(conj);
                } else {
                  removed = true;
                }
                continue;
              }
            }
            kept.push_back(conj);
          }

          if (removed && !kept.empty()) {
            LogicalExpression rewritten = expression;
            rewritten.predicate = CanonicalizeConjuncts(CombineConjuncts(kept));
            memo.AddExpression(group, std::move(rewritten));
          }
        },
        LogicalOperator::kJoin));

    // unnest_filter_pushdown: Push selection filter down into the child input
    // of Unnest.
    built.Add(Rule(
        "unnest_filter_pushdown", Selection(Unnest(Any(), "unnest")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          const GroupId unnest_group_id = bindings.at("unnest");
          if (unnest_group_id == group) {
            return;
          }
          const Group& unnest_group = memo.Get(unnest_group_id);
          for (const LogicalExpression& unnest : unnest_group.expressions) {
            if (unnest.operation != LogicalOperator::kUnnest ||
                unnest.children.size() != 1 || unnest.children[0] == group) {
              continue;
            }
            if (expression.predicate && *expression.predicate) {
              const std::unordered_set<ColumnName> touched =
                  (*expression.predicate)->TouchedColumns();
              bool touches_unnest = false;
              for (const auto& col : touched) {
                if ((!unnest.unnest_alias.empty() &&
                     IdentifierEquals(col.name, unnest.unnest_alias)) ||
                    (!unnest.offset_alias.empty() &&
                     IdentifierEquals(col.name, unnest.offset_alias))) {
                  touches_unnest = true;
                  break;
                }
              }
              if (touches_unnest) {
                continue;
              }
            }
            const GroupId unnest_child = unnest.children[0];
            const GroupId sel_below = memo.EnsureDerivedGroup(
                memo.Get(unnest_child).relations,
                "sel_unnest_child:" + GroupIdsTag({unnest_child}) +
                    PredicateTag(expression.predicate));
            InheritBranchLocal(memo, sel_below, group);
            if (sel_below != unnest_child && sel_below != group) {
              memo.AddExpression(
                  sel_below, LogicalExpression{
                                 .operation = LogicalOperator::kSelection,
                                 .children = {unnest_child},
                                 .predicate = expression.predicate,
                                 .output_schema =
                                     memo.Get(unnest_child).expressions.empty()
                                         ? Schema()
                                         : memo.Get(unnest_child)
                                               .expressions.front()
                                               .output_schema});
              LogicalExpression new_unnest = unnest;
              new_unnest.children = {sel_below};
              memo.AddExpression(group, std::move(new_unnest));
            }
            return;
          }
        },
        LogicalOperator::kSelection));

    // constant_unnest_to_values: A stand-alone UNNEST over a one-row leaf
    // (the dummy that a FROM-level UNNEST gets when no other source exists)
    // whose array argument folds to a constant IS the expanded element list,
    // with each row = [element, offset?].  Fold it into a kValues node.
    // Refusals keep the executor path: nested-array element layouts,
    // non-constant shapes the pure evaluator cannot fold, struct/PROTO
    // widening layouts the rule cannot mirror, driving inputs wider than
    // the dummy leaf, and beyond-threshold element counts
    // (kConstantUnnestValuesThreshold).
    built.Add(Rule(
        "constant_unnest_to_values", Unnest(Any("input"), "unnest"),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          const Expression& predicate = *expression.predicate;
          // Only shapes the runtime evaluator treats as deterministic
          // constants: a literal (possibly folded earlier by constant
          // folding), an ARRAY literal of constants, or a GENERATE_SERIES
          // over constants.  A generative builtin like CURRENT_DATE must
          // not freeze into a plan-cached literal.
          if (predicate->Type() == TypeTag::kFunctionCallExp) {
            if (!IdentifierEquals(
                    "GENERATE_SERIES",
                    predicate->AsFunctionCallExpression().FuncName())) {
              return;
            }
          } else if (predicate->Type() == TypeTag::kArrayExp) {
            bool constant_children = true;
            for (const Expression& child : ExpressionChildren(predicate)) {
              if (!child || child->Type() != TypeTag::kConstantValue) {
                constant_children = false;
                break;
              }
            }
            if (!constant_children) {
              return;
            }
          } else if (predicate->Type() != TypeTag::kConstantValue) {
            return;
          }
          const StatusOr<Value> evaluated =
              predicate->TryEvaluate(Row(), Schema());
          if (!evaluated.HasValue() || !evaluated.Value().IsArray()) {
            return;
          }
          const std::vector<Value> elements = evaluated.Value().ArrayElements();
          if (elements.size() > kConstantUnnestValuesThreshold) {
            return;
          }
          for (const Value& element : elements) {
            if (element.IsArray()) {
              return;
            }
          }
          // The fold reproduces the executor's scalar layout (elem column
          // then the 0-based counter) exactly; a section schema that
          // disagrees comes from a struct/PROTO element layout.
          const bool with_offset = !expression.offset_alias.empty();
          const size_t expected_width = with_offset ? 2U : 1U;
          const std::string rel_name = expression.unnest_alias.empty()
                                           ? "unnest"
                                           : expression.unnest_alias;
          Schema section = expression.output_schema;
          if (section.ColumnCount() == expected_width) {
            for (size_t i = 0; i < section.ColumnCount(); ++i) {
              if (!IdentifierEquals(section.GetColumn(i).Name().schema,
                                    rel_name)) {
                return;
              }
            }
          } else if (section.ColumnCount() != 0) {
            return;
          } else {
            ValueType element_type = ValueType::kNull;
            for (const Value& element : elements) {
              if (!element.IsNull()) {
                element_type = element.type;
                break;
              }
            }
            std::vector<Column> derived;
            derived.emplace_back(ColumnName(rel_name, rel_name), element_type);
            if (with_offset) {
              derived.emplace_back(
                  ColumnName(rel_name, expression.offset_alias),
                  ValueType::kInt64);
            }
            section = Schema(rel_name, std::move(derived));
          }
          // Only a one-row driving leaf folds to plain Values.  The dummy
          // group owns the FROM-level route; VALUES leaves never reach the
          // unnest builder today, so treat anything else as unsupported.
          bool dummy_leaf = false;
          if (bindings.at("input") != group &&
              !memo.Get(bindings.at("input")).expressions.empty()) {
            dummy_leaf = std::ranges::all_of(
                memo.Get(bindings.at("input")).expressions,
                [](const LogicalExpression& leaf) {
                  return leaf.operation == LogicalOperator::kDummyScan;
                });
          }
          if (!dummy_leaf) {
            return;
          }
          for (const LogicalExpression& existing :
               memo.Get(group).expressions) {
            if (existing.operation == LogicalOperator::kValues) {
              return;
            }
          }
          std::vector<Row> folded;
          folded.reserve(elements.size());
          for (size_t index = 0; index < elements.size(); ++index) {
            folded.emplace_back(
                with_offset
                    ? Row({elements[index], Value(static_cast<int64_t>(index))})
                    : Row({elements[index]}));
          }
          LogicalExpression values_expression{};
          values_expression.operation = LogicalOperator::kValues;
          values_expression.values = std::move(folded);
          values_expression.output_schema = std::move(section);
          memo.AddExpression(group, std::move(values_expression));
        },
        LogicalOperator::kUnnest));

    // recursive_termination_predicate_pushdown: narrow a recursive CTE's
    // depth window from a Selection directly above it. The opaque leaf
    // (childless kRecursiveCte with a depth column, the production shape
    // emitted by LiftRecursiveCtes) emits exactly the fixpoint rows whose
    // depth lies in [lower, upper], and every row at depth <= N is fully
    // produced by iteration N -- so filtering `depth <= N` post-hoc equals
    // capping the driver at depth N. Only comparisons of the depth column
    // against integer constants are sound here: any other predicate would
    // also drop rows the recursion needed as working set (a non-monotone
    // filter changes the fixpoint itself), so those keep the Selection.
    built.Add(Rule(
        "recursive_termination_predicate_pushdown", Selection(Any("cte")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId cte_group_id = bindings.at("cte");
          if (cte_group_id == group) {
            return;
          }
          const Group& cte_group = memo.Get(cte_group_id);
          for (const LogicalExpression& cte : cte_group.expressions) {
            if (cte.operation != LogicalOperator::kRecursiveCte ||
                !cte.children.empty() || !cte.depth_spec.has_value()) {
              continue;
            }
            const RecursiveDepthSpec& spec = *cte.depth_spec;
            int64_t lower = spec.lower;
            int64_t upper = spec.upper;
            bool narrowed = false;
            bool recognized = true;
            for (const Expression& conjunct :
                 SplitConjuncts(*expression.predicate)) {
              if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
                recognized = false;
                break;
              }
              const auto& bin = conjunct->AsBinaryExpression();
              // The depth test is `depth_col <op> int_const` either way
              // around; anything else keeps the Selection.
              const bool left_is_depth =
                  bin.Left()->Type() == TypeTag::kColumnValue &&
                  IdentifierEquals(
                      bin.Left()->AsColumnValue().GetColumnName().name,
                      spec.column) &&
                  (bin.Left()->AsColumnValue().GetColumnName().schema.empty() ||
                   IdentifierEquals(
                       bin.Left()->AsColumnValue().GetColumnName().schema,
                       cte.cte_name));
              const bool right_is_depth =
                  bin.Right()->Type() == TypeTag::kColumnValue &&
                  IdentifierEquals(
                      bin.Right()->AsColumnValue().GetColumnName().name,
                      spec.column) &&
                  (bin.Right()
                       ->AsColumnValue()
                       .GetColumnName()
                       .schema.empty() ||
                   IdentifierEquals(
                       bin.Right()->AsColumnValue().GetColumnName().schema,
                       cte.cte_name));
              const ExpressionBase* constant = nullptr;
              bool depth_on_left = false;
              if (left_is_depth &&
                  bin.Right()->Type() == TypeTag::kConstantValue) {
                constant = bin.Right().get();
                depth_on_left = true;
              } else if (right_is_depth &&
                         bin.Left()->Type() == TypeTag::kConstantValue) {
                constant = bin.Left().get();
              } else {
                recognized = false;
                break;
              }
              const Value& bound = constant->AsConstantValue().GetValue();
              if (bound.IsNull() || bound.type != ValueType::kInt64) {
                recognized = false;
                break;
              }
              const int64_t n = bound.value.int_value;
              BinaryOperation op = bin.Op();
              if (!depth_on_left) {
                // `const <op> depth` mirrors to `depth <op'> const`.
                switch (op) {
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
                    break;
                }
              }
              switch (op) {
                case BinaryOperation::kLessThanEquals:
                  if (n < upper) {
                    upper = n;
                    narrowed = true;
                  }
                  break;
                case BinaryOperation::kLessThan:
                  if (n == std::numeric_limits<int64_t>::min()) {
                    recognized = false;
                    break;
                  }
                  if (n - 1 < upper) {
                    upper = n - 1;
                    narrowed = true;
                  }
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  if (n > lower) {
                    lower = n;
                    narrowed = true;
                  }
                  break;
                case BinaryOperation::kGreaterThan:
                  if (n == std::numeric_limits<int64_t>::max()) {
                    recognized = false;
                    break;
                  }
                  if (n + 1 > lower) {
                    lower = n + 1;
                    narrowed = true;
                  }
                  break;
                default:
                  recognized = false;
                  break;
              }
              if (!recognized) {
                break;
              }
            }
            if (!recognized || !narrowed || lower > upper) {
              continue;
            }
            LogicalExpression narrowed_cte = cte;
            narrowed_cte.depth_spec = RecursiveDepthSpec{
                .column = spec.column, .lower = lower, .upper = upper};
            memo.AddExpression(group, std::move(narrowed_cte));
            return;
          }
        },
        LogicalOperator::kSelection));

    // in_list_to_semi_join: Transform large IN list predicates into SemiJoin
    // with Values/ConstantTable.
    built.Add(Rule(
        "in_list_to_semi_join", Selection(Any("child")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate) {
            return;
          }
          const GroupId child_id = bindings.at("child");
          if (child_id == group) {
            return;
          }
          std::vector<Value> in_values;
          std::optional<ColumnName> target_col;
          // D5 gate ("IN-list semi join: 収集できなかったOR枝を捨てず、全枝を
          // 保持する"): if ANY branch of the OR tree is not a col=const
          // equality on the same column, the constant table would silently
          // drop that branch's rows.  Track completeness and refuse to fire
          // instead of rewriting to a smaller row set.
          bool collected_all_branches = true;
          const auto collect_in_values = [&](auto& self,
                                             const Expression& exp) -> void {
            if (!exp) {
              collected_all_branches = false;
              return;
            }
            if (exp->Type() == TypeTag::kBinaryExp) {
              const auto& bin = exp->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kOr) {
                self(self, bin.Left());
                self(self, bin.Right());
                return;
              }
              if (bin.Op() == BinaryOperation::kEquals) {
                const bool left_col =
                    bin.Left()->Type() == TypeTag::kColumnValue &&
                    bin.Right()->Type() == TypeTag::kConstantValue;
                const bool right_col =
                    bin.Right()->Type() == TypeTag::kColumnValue &&
                    bin.Left()->Type() == TypeTag::kConstantValue;
                if (left_col || right_col) {
                  const ColumnName col = (left_col ? bin.Left() : bin.Right())
                                             ->AsColumnValue()
                                             .GetColumnName();
                  const Value constant = (left_col ? bin.Right() : bin.Left())
                                             ->AsConstantValue()
                                             .GetValue();
                  if (constant.IsNull() ||
                      (!in_values.empty() &&
                       constant.type != in_values.front().type)) {
                    collected_all_branches = false;
                    return;
                  }
                  if (!target_col) {
                    target_col = col;
                  }
                  if (*target_col == col) {
                    in_values.push_back(constant);
                    return;
                  }
                }
              }
            }
            collected_all_branches = false;
          };
          collect_in_values(collect_in_values, *expression.predicate);

          if (!collected_all_branches || !target_col || in_values.size() < 3) {
            return;
          }
          const std::string const_table_name = "in_const_" + target_col->name;
          // D5 gate ("derived group: 意味内容を含むfingerprintで識別する"):
          // the group tag fingerprints the column AND every constant, so a
          // later list with different values can never reuse this group.
          std::string value_fingerprint = target_col->ToString() + ":";
          for (const Value& v : in_values) {
            value_fingerprint += v.AsString();
            value_fingerprint.push_back(';');
          }
          const GroupId const_group =
              memo.EnsureDerivedGroup({}, "in_list_table:" + value_fingerprint);
          if (const_group != group) {
            std::vector<Row> rows;
            rows.reserve(in_values.size());
            for (const auto& v : in_values) {
              rows.push_back(Row({v}));
            }
            LogicalExpression values_expression;
            values_expression.operation = LogicalOperator::kValues;
            values_expression.target_list = {NamedExpression(
                target_col->name, ColumnValueExp(ColumnName(
                                      const_table_name, target_col->name)))};
            values_expression.values = std::move(rows);
            values_expression.output_schema =
                Schema(const_table_name,
                       {Column(target_col->name, in_values.front().type)});
            memo.AddExpression(const_group, std::move(values_expression));

            const Expression join_cond = BinaryExpressionExp(
                ColumnValueExp(*target_col), BinaryOperation::kEquals,
                ColumnValueExp(ColumnName(const_table_name, target_col->name)));

            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                  .children = {child_id, const_group},
                                  .predicate = join_cond,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kSelection));

    // functional_dependency_filter_reduction: Simplify filters using known
    // functional dependencies and equality keys.
    built.Add(Rule(
        "functional_dependency_filter_reduction", Selection(Any("child")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate) {
            return;
          }
          const auto conjuncts = SplitConjuncts(*expression.predicate);
          if (conjuncts.size() < 2) {
            return;
          }
          std::unordered_map<std::string, Value> eq_constants;
          for (const auto& conj : conjuncts) {
            if (conj && conj->Type() == TypeTag::kBinaryExp &&
                !ContainsParameterSlot(conj)) {
              const auto& bin = conj->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kEquals) {
                if (bin.Left()->Type() == TypeTag::kColumnValue &&
                    bin.Right()->Type() == TypeTag::kConstantValue) {
                  const Value& val = bin.Right()->AsConstantValue().GetValue();
                  if (!val.IsNull()) {
                    eq_constants.try_emplace(
                        bin.Left()->AsColumnValue().GetColumnName().ToString(),
                        val);
                  }
                } else if (bin.Right()->Type() == TypeTag::kColumnValue &&
                           bin.Left()->Type() == TypeTag::kConstantValue) {
                  const Value& val = bin.Left()->AsConstantValue().GetValue();
                  if (!val.IsNull()) {
                    eq_constants.try_emplace(
                        bin.Right()->AsColumnValue().GetColumnName().ToString(),
                        val);
                  }
                }
              }
            }
          }
          if (eq_constants.empty()) {
            return;
          }
          std::vector<Expression> kept;
          bool reduced = false;
          for (const auto& conj : conjuncts) {
            if (conj && conj->Type() == TypeTag::kBinaryExp &&
                !ContainsParameterSlot(conj)) {
              const auto& bin = conj->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kLessThanEquals ||
                  bin.Op() == BinaryOperation::kGreaterThanEquals ||
                  bin.Op() == BinaryOperation::kLessThan ||
                  bin.Op() == BinaryOperation::kGreaterThan) {
                if (bin.Left()->Type() == TypeTag::kColumnValue &&
                    bin.Right()->Type() == TypeTag::kConstantValue) {
                  const std::string col_str =
                      bin.Left()->AsColumnValue().GetColumnName().ToString();
                  auto it = eq_constants.find(col_str);
                  if (it != eq_constants.end()) {
                    const Value& k = it->second;
                    const Value& bound =
                        bin.Right()->AsConstantValue().GetValue();
                    if (!k.IsNull() && !bound.IsNull() &&
                        k.type == bound.type) {
                      const StatusOr<Value> eval =
                          TryEvaluateBinary(bin.Op(), k, bound);
                      if (eval.HasValue() && !eval.Value().IsNull() &&
                          eval.Value().Truthy()) {
                        reduced = true;
                        continue;
                      }
                    }
                  }
                }
              }
            }
            kept.push_back(conj);
          }
          if (reduced && !kept.empty()) {
            memo.AddExpression(
                group,
                LogicalExpression{
                    .operation = LogicalOperator::kSelection,
                    .children = expression.children,
                    .predicate = CanonicalizeConjuncts(CombineConjuncts(kept)),
                    .target_list = expression.target_list,
                    .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kSelection));
    // filter_aggregate_pushdown: Push filtering conditions directly into
    // aggregate FILTER (WHERE ...) clauses where advantageous.
    built.Add(Rule(
        "filter_aggregate_pushdown",
        Aggregation(Selection(Any("input"), "sel")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.target_list.empty()) {
            return;
          }
          const GroupId sel_id = bindings.at("sel");
          if (sel_id == group) {
            return;
          }
          const Group& sel_group = memo.Get(sel_id);
          for (const auto& sel_expr : sel_group.expressions) {
            if (sel_expr.operation != LogicalOperator::kSelection ||
                sel_expr.children.empty() || !sel_expr.predicate ||
                !*sel_expr.predicate) {
              continue;
            }
            const GroupId input_id = sel_expr.children[0];
            if (input_id == group || input_id == sel_id) {
              continue;
            }
            // Only scalar aggregation may move the selection into FILTER
            // clauses: with GROUP BY, groups whose every row fails the
            // predicate vanish from the original plan but would survive the
            // rewrite as COUNT=0 phantom groups.
            if (!expression.grouping_sets.empty()) {
              continue;
            }
            // The predicate is replicated into every aggregate's FILTER
            // clause, multiplying its evaluation count by the number of
            // aggregates. Volatile functions or subqueries would change
            // results (or cost), so such selections stay in place.
            if (!SafeToReduceEvaluationCount(*sel_expr.predicate)) {
              continue;
            }
            std::vector<NamedExpression> new_targets;
            bool transformed = false;
            for (const auto& target : expression.target_list) {
              if (target.expression &&
                  target.expression->Type() == TypeTag::kAggregateExp) {
                const auto& orig =
                    static_cast<const AggregateExpression&>(*target.expression);
                auto agg_copy = CloneAggregate(orig, orig.Distinct());
                // Conjoin with any existing FILTER instead of silently
                // overwriting it.
                if (orig.HasWhereFilter()) {
                  agg_copy->SetWhereFilter(BinaryExpressionExp(
                      orig.WhereFilter(), BinaryOperation::kAnd,
                      *sel_expr.predicate));
                } else {
                  agg_copy->SetWhereFilter(*sel_expr.predicate);
                }
                new_targets.emplace_back(target.name, agg_copy);
                transformed = true;
              } else {
                new_targets.push_back(target);
              }
            }
            if (transformed) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {input_id},
                                    .target_list = std::move(new_targets),
                                    .output_schema = expression.output_schema,
                                    .partition_by = expression.partition_by,
                                    .grouping_sets = expression.grouping_sets});
            }
            break;
          }
        },
        LogicalOperator::kAggregation));

    // aggregate_union_transpose: Push Aggregation below UnionAll when
    // aggregates are distributable (SUM, COUNT, MIN, MAX).
    built.Add(Rule(
        "aggregate_union_transpose", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1 ||
              expression.target_list.empty()) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          // Fixpoint guard: the re-aggregation this rule emits sits over a
          // union of already-pushed aggregates; pushing it again would nest
          // aggregate-of-aggregate groups without bound.
          if (memo.Get(input_id).tag.starts_with("union_all_pushed_aggs:")) {
            return;
          }
          const Group& input_group = memo.Get(input_id);
          for (const auto& union_expr : input_group.expressions) {
            if (union_expr.operation != LogicalOperator::kUnionAll ||
                union_expr.children.empty()) {
              continue;
            }

            bool distributable = true;
            for (const auto& target : expression.target_list) {
              if (!target.expression) {
                distributable = false;
                break;
              }
              if (target.expression->Type() == TypeTag::kAggregateExp) {
                const auto& agg =
                    static_cast<const AggregateExpression&>(*target.expression);
                if (agg.Distinct() ||
                    agg.Having() != AggregateHavingModifier::kNone ||
                    agg.WhereFilter()) {
                  distributable = false;
                  break;
                }
                if (agg.GetType() != AggregationType::kSum &&
                    agg.GetType() != AggregationType::kCount &&
                    agg.GetType() != AggregationType::kMin &&
                    agg.GetType() != AggregationType::kMax) {
                  distributable = false;
                  break;
                }
              }
            }
            if (!distributable) {
              continue;
            }

            std::vector<GroupId> pushed_aggs;
            pushed_aggs.reserve(union_expr.children.size());
            for (size_t i = 0; i < union_expr.children.size(); ++i) {
              const GroupId branch_id = union_expr.children[i];
              // Keyed by the branch group itself (D1): two unions over the
              // same relation set must never share a per-branch aggregate.
              const GroupId branch_agg = memo.EnsureDerivedGroup(
                  memo.Get(branch_id).relations,
                  "agg_union_branch_" + std::to_string(i) + ":" +
                      std::to_string(branch_id) + ":" +
                      TargetListFingerprint(expression.target_list));
              if (branch_agg != branch_id && branch_agg != group) {
                memo.AddExpression(
                    branch_agg, LogicalExpression{
                                    .operation = LogicalOperator::kAggregation,
                                    .children = {branch_id},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema,
                                    .partition_by = expression.partition_by,
                                    .grouping_sets = expression.grouping_sets});
                pushed_aggs.push_back(branch_agg);
              }
            }
            if (pushed_aggs.size() != union_expr.children.size()) {
              continue;
            }

            // A constant tag made every aggregation over this relation set
            // share ONE union group; fingerprint the pushed branches.
            std::string union_tag = "union_all_pushed_aggs:";
            for (const GroupId pushed : pushed_aggs) {
              union_tag += std::to_string(pushed);
              union_tag += ',';
            }
            const GroupId union_of_aggs =
                memo.EnsureDerivedGroup(memo.Get(group).relations, union_tag);
            if (union_of_aggs != group && union_of_aggs != input_id) {
              memo.AddExpression(
                  union_of_aggs,
                  LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                    .children = pushed_aggs,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});

              std::vector<NamedExpression> final_targets;
              for (const auto& target : expression.target_list) {
                if (target.expression &&
                    target.expression->Type() == TypeTag::kAggregateExp) {
                  const auto& agg = static_cast<const AggregateExpression&>(
                      *target.expression);
                  if (agg.GetType() == AggregationType::kCount) {
                    final_targets.emplace_back(
                        target.name,
                        AggregateExpressionExp(
                            AggregationType::kSum,
                            ColumnValueExp(ColumnName(target.name))));
                  } else {
                    final_targets.emplace_back(
                        target.name,
                        AggregateExpressionExp(
                            agg.GetType(),
                            ColumnValueExp(ColumnName(target.name))));
                  }
                } else {
                  final_targets.push_back(target);
                }
              }

              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {union_of_aggs},
                                    .target_list = std::move(final_targets),
                                    .output_schema = expression.output_schema,
                                    .partition_by = expression.partition_by,
                                    .grouping_sets = expression.grouping_sets});
            }
          }
        },
        LogicalOperator::kAggregation));

    // pk_unique_distinct_elimination: Eliminate DISTINCT when input columns
    // form a primary key or unique key.
    built.Add(Rule(
        "pk_unique_distinct_elimination", Distinct(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kDistinct ||
              expression.children.size() != 1) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const Group& input_group = memo.Get(input_id);
          // SQL's Distinct carries no target list. Prove uniqueness of the
          // actual child's output, mapping a projection back to its source
          // keys, and reuse that equivalent expression without another pass.
          // Copy candidates because AddExpression may invalidate storage.
          const auto candidates = input_group.expressions;
          for (const auto& child : candidates) {
            const LogicalProperties* properties =
                &input_group.logical_properties;
            std::unordered_set<std::string> columns;
            if (child.operation == LogicalOperator::kProjection &&
                child.children.size() == 1 && child.children[0] != group) {
              properties = &memo.Get(child.children[0]).logical_properties;
              for (const auto& target : child.target_list) {
                if (target.expression &&
                    target.expression->Type() == TypeTag::kColumnValue) {
                  columns.insert(target.expression->AsColumnValue()
                                     .GetColumnName()
                                     .ToString());
                }
              }
            } else {
              const Schema* schema = &child.output_schema;
              if (child.operation == LogicalOperator::kScan &&
                  schema->ColumnCount() == 0) {
                const auto found = memo.GetTableSchemas().find(child.table);
                if (found != memo.GetTableSchemas().end()) {
                  schema = &found->second;
                }
              }
              for (size_t i = 0; i < schema->ColumnCount(); ++i) {
                columns.insert(schema->GetColumn(i).Name().ToString());
              }
            }
            if (!columns.empty() && properties->IsUniqueOn(columns)) {
              if (child.operation == LogicalOperator::kScan &&
                  input_group.filter) {
                memo.MergeScanFilter(group, input_group.filter);
              }
              memo.AddExpression(group, child);
            }
          }
        },
        LogicalOperator::kDistinct));

    // not_null_is_not_null_elimination: Rewrite col IS NOT NULL to TRUE when
    // col has a NOT NULL constraint.
    built.Add(Rule(
        "not_null_is_not_null_elimination", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const Group& input_group = memo.Get(input_id);

          const auto conjuncts = SplitConjuncts(*expression.predicate);
          std::vector<Expression> kept;
          bool changed = false;
          for (const auto& conj : conjuncts) {
            if (conj && conj->Type() == TypeTag::kUnaryExp &&
                conj->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull) {
              const auto& child = conj->AsUnaryExpression().Child();
              if (child && child->Type() == TypeTag::kColumnValue) {
                const ColumnName& col_name =
                    child->AsColumnValue().GetColumnName();
                if (input_group.logical_properties.IsNotNull(
                        col_name.ToString())) {
                  changed = true;
                  continue;
                }
              }
            }
            kept.push_back(conj);
          }

          if (changed) {
            if (kept.empty()) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {input_id},
                                    .predicate = ConstantValueExp(Value(true)),
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            } else {
              memo.AddExpression(
                  group, LogicalExpression{
                             .operation = LogicalOperator::kSelection,
                             .children = {input_id},
                             .predicate =
                                 CanonicalizeConjuncts(CombineConjuncts(kept)),
                             .target_list = expression.target_list,
                             .output_schema = expression.output_schema});
            }
          }
        },
        LogicalOperator::kSelection));

    // join_identity_dummy: CrossJoin(X, DummyScan) -> X. DummyScan yields
    // exactly one row with no columns, so the cross product preserves X
    // row-for-row (and symmetrically for a leading DummyScan). Inner joins
    // without a predicate reach this shape through
    // join_to_cross_if_no_predicate, so only the CrossJoin form is needed.
    built.Add(Rule(
        "join_identity_dummy", CrossJoin(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kCrossJoin ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId left_id = bindings.at("left");
          const GroupId right_id = bindings.at("right");
          const auto is_dummy = [&](GroupId id) {
            return std::ranges::any_of(memo.Get(id).expressions,
                                       [](const LogicalExpression& candidate) {
                                         return candidate.operation ==
                                                LogicalOperator::kDummyScan;
                                       });
          };
          const bool left_dummy = is_dummy(left_id);
          const bool right_dummy = is_dummy(right_id);
          if (left_dummy == right_dummy) {
            return;
          }
          const GroupId survivor = left_dummy ? right_id : left_id;
          if (memo.Get(survivor).relations != memo.Get(group).relations) {
            return;
          }
          for (const LogicalExpression& alternative :
               memo.Get(survivor).expressions) {
            bool cycle = false;
            for (GroupId child : alternative.children) {
              if (child == group) {
                cycle = true;
                break;
              }
            }
            if (!cycle) {
              memo.AddExpression(group, alternative);
            }
          }
        },
        LogicalOperator::kCrossJoin));

    // count_star_rewrite_on_not_null: COUNT(col) -> COUNT(*) when col is
    // provably NOT NULL. COUNT skips NULLs, so on a non-nullable column it
    // counts exactly the input rows and saves per-row null checks.
    // The input group must retain a derived not-null proof: a catalog
    // constraint alone does not survive NULL extension or projection.
    // Aggregates with DISTINCT,
    // FILTER, HAVING modifiers or inner ORDER BY / LIMIT keep their shape.
    built.Add(Rule(
        "count_star_rewrite_on_not_null", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1 ||
              expression.target_list.empty()) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const Group& input_group = memo.Get(input_id);
          bool changed = false;
          std::vector<NamedExpression> rewritten_targets;
          rewritten_targets.reserve(expression.target_list.size());
          for (const NamedExpression& target : expression.target_list) {
            if (!target.expression ||
                target.expression->Type() != TypeTag::kAggregateExp) {
              rewritten_targets.push_back(target);
              continue;
            }
            const auto& agg = target.expression->AsAggregateExpression();
            if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
                agg.WhereFilter() ||
                agg.Having() != AggregateHavingModifier::kNone ||
                !agg.InnerOrderBy().empty() || agg.InnerLimit().has_value() ||
                !agg.TrailingArgs().empty() || agg.SecondaryArg() ||
                !agg.Child() || agg.Child()->Type() != TypeTag::kColumnValue) {
              rewritten_targets.push_back(target);
              continue;
            }
            const ColumnName& column =
                agg.Child()->AsColumnValue().GetColumnName();
            if (column.schema.empty()) {
              rewritten_targets.push_back(target);
              continue;
            }
            // The input group is the proof boundary: a base declaration
            // does not survive a NULL-supplying outer-join side.
            if (!input_group.logical_properties.IsNotNull(column.ToString())) {
              rewritten_targets.push_back(target);
              continue;
            }
            NamedExpression star = target;
            star.expression =
                AggregateExpressionExp(AggregationType::kCount, nullptr, false);
            rewritten_targets.push_back(std::move(star));
            changed = true;
          }
          if (!changed) {
            return;
          }
          LogicalExpression rewritten = expression;
          rewritten.target_list = std::move(rewritten_targets);
          memo.AddExpression(group, std::move(rewritten));
        },
        LogicalOperator::kAggregation));

    // topn_push_through_inner_join is DISABLED (see the disabled-rules table
    // in docs/cascades_optimizer.md): pushing TopN below an inner join
    // requires proof that every left row finds a match, but uniqueness on
    // the right join keys only bounds the match count at one. With an
    // unmatched left row inside the top-(limit+offset) prefix, the rewritten
    // plan drops rows the original plan keeps. Proving ">= 1 match" needs
    // referential-integrity metadata the memo does not model.

    // topn_push_through_proven_one_to_one_join (TODO.md item 1c/1d):
    // TopN(keys, limit, offset) over an inner Join(L, R) gains an alternative
    // Join(TopN(keys, limit+offset), R) when the join is proven 1:1 from the
    // pushed side S (every S row matches exactly one other-side row) and the
    // TopN keys are evaluable on S alone. Then the global top-(limit+offset)
    // joined rows correspond 1:1 to S's top-(limit+offset) rows, and the
    // parent TopN(limit, offset) re-establishes the exact output.
    // The 1:1 proof is either a declared FOREIGN KEY from S to the other
    // side (scan-free catalog proof)
    // or a snapshot proof recorded by the optimizer (Memo::IsProvenOneToOne).
    // Uniqueness alone never suffices (see the disabled rule above); WITH
    // TIES is excluded because tied peers can overflow the pushed cap.
    built.Add(Rule(
        "topn_push_through_proven_one_to_one_join", TopN(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kTopN ||
              expression.children.size() != 1 || expression.with_ties) {
            return;
          }
          if (expression.limit_offset >
              std::numeric_limits<size_t>::max() - expression.limit_count) {
            return;
          }
          const size_t total_limit =
              expression.limit_count + expression.limit_offset;
          if (total_limit == 0 || expression.target_list.empty() ||
              expression.target_list.size() !=
                  expression.sort_ascending.size()) {
            return;
          }
          // Snapshot everything the rewrite needs before mutating the memo:
          // AddExpression/EnsureDerivedGroup may reallocate group storage,
          // so no Group/expression reference may survive past them.
          const std::vector<NamedExpression> topn_keys = expression.target_list;
          const std::vector<bool> topn_ascending = expression.sort_ascending;
          const std::vector<std::optional<bool>> topn_nulls_first =
              expression.sort_nulls_first;
          const size_t topn_limit = expression.limit_count;
          const size_t topn_offset = expression.limit_offset;
          const GroupId input_id = bindings.at("input");
          const std::vector<LogicalExpression> join_candidates =
              memo.Get(input_id).expressions;
          const std::vector<std::string> input_relations =
              memo.Get(input_id).relations;
          for (const LogicalExpression& join_expr : join_candidates) {
            if (join_expr.operation != LogicalOperator::kJoin ||
                join_expr.children.size() != 2 ||
                !join_expr.predicate.has_value() || !*join_expr.predicate) {
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            const std::vector<std::string> left_relations =
                memo.Get(left_id).relations;
            const std::vector<std::string> right_relations =
                memo.Get(right_id).relations;
            // v1 scope: two single-relation sides (the optimizer only marks
            // proofs for two-relation queries).
            if (left_relations.size() != 1 || right_relations.size() != 1) {
              continue;
            }
            const Expression join_predicate = *join_expr.predicate;
            for (int side = 0; side < 2; ++side) {
              const GroupId pushed_id = side == 0 ? left_id : right_id;
              const GroupId other_id = side == 0 ? right_id : left_id;
              const std::string pushed_rel =
                  side == 0 ? left_relations.front() : right_relations.front();
              const std::string other_rel =
                  side == 0 ? right_relations.front() : left_relations.front();
              // TopN keys must resolve to the pushed side alone; unqualified
              // names cannot be proven local (strict interpretation, same as
              // PayloadConstraint::predicate_within_child).
              bool keys_local = true;
              for (const NamedExpression& key : topn_keys) {
                if (!key.expression) {
                  keys_local = false;
                  break;
                }
                for (const ColumnName& column :
                     key.expression->TouchedColumns()) {
                  if (column.schema.empty() || column.schema != pushed_rel) {
                    keys_local = false;
                    break;
                  }
                }
                if (!keys_local) {
                  break;
                }
              }
              if (!keys_local) {
                continue;
              }
              // Collect equi-join columns per side.
              std::unordered_set<std::string> pushed_join_cols;
              std::unordered_set<std::string> other_join_cols;
              size_t equi_join_count = 0;
              const std::vector<Expression> join_conjuncts =
                  SplitConjuncts(join_predicate);
              for (const Expression& conjunct : join_conjuncts) {
                if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
                  continue;
                }
                const auto& binary = conjunct->AsBinaryExpression();
                if (binary.Op() != BinaryOperation::kEquals ||
                    binary.Left()->Type() != TypeTag::kColumnValue ||
                    binary.Right()->Type() != TypeTag::kColumnValue) {
                  continue;
                }
                const ColumnName& left_col =
                    binary.Left()->AsColumnValue().GetColumnName();
                const ColumnName& right_col =
                    binary.Right()->AsColumnValue().GetColumnName();
                const ColumnName* pushed_col = nullptr;
                const ColumnName* other_col = nullptr;
                if (left_col.schema == pushed_rel &&
                    right_col.schema == other_rel) {
                  pushed_col = &left_col;
                  other_col = &right_col;
                } else if (left_col.schema == other_rel &&
                           right_col.schema == pushed_rel) {
                  pushed_col = &right_col;
                  other_col = &left_col;
                } else {
                  continue;
                }
                pushed_join_cols.insert(pushed_col->ToString());
                pushed_join_cols.insert(pushed_col->name);
                other_join_cols.insert(other_col->ToString());
                other_join_cols.insert(other_col->name);
                ++equi_join_count;
              }
              if (pushed_join_cols.empty() || other_join_cols.empty() ||
                  equi_join_count != join_conjuncts.size()) {
                continue;
              }
              // At most one match per pushed row: the other-side join keys
              // are unique and NOT NULL (NULL keys never match, and a
              // nullable UNIQUE admits duplicate NULLs, so uniqueness alone
              // is not a proof; mirrors DeriveLogicalProperties).
              if (!memo.Get(other_id).logical_properties.IsUniqueOn(
                      other_join_cols)) {
                continue;
              }
              bool other_not_null = true;
              for (const std::string& column : other_join_cols) {
                if (!memo.Get(other_id).logical_properties.IsNotNull(column)) {
                  other_not_null = false;
                  break;
                }
              }
              if (!other_not_null) {
                continue;
              }
              // At least one match per pushed row: FOREIGN KEY or snapshot.
              bool proven = memo.IsProvenOneToOne(pushed_rel, other_rel);
              if (!proven) {
                const std::vector<LogicalExpression> pushed_exprs =
                    memo.Get(pushed_id).expressions;
                for (const auto& candidate : pushed_exprs) {
                  for (size_t i = 0; i < candidate.output_schema.ColumnCount();
                       ++i) {
                    const auto& column = candidate.output_schema.GetColumn(i);
                    if (!pushed_join_cols.contains(column.Name().ToString()) &&
                        !pushed_join_cols.contains(column.Name().name)) {
                      continue;
                    }
                    if (column.GetConstraint().ctype != Constraint::kForeign) {
                      continue;
                    }
                    std::string ref_table;
                    std::vector<std::string> ref_columns;
                    if (!ParseForeignKeyReference(column.GetConstraint(),
                                                  &ref_table, &ref_columns)) {
                      continue;
                    }
                    bool references_join_keys =
                        ref_columns.size() == join_conjuncts.size();
                    for (const std::string& ref_column : ref_columns) {
                      references_join_keys =
                          references_join_keys &&
                          other_join_cols.contains(ref_column);
                    }
                    if (ref_table == other_rel && references_join_keys) {
                      // The FK certificate covers only non-NULL values
                      // (NULL keys match nothing), so without a NOT NULL
                      // proof on the pushed side an unmatched row inside
                      // the top-(limit+offset) prefix drops rows the
                      // unpushed plan keeps -- the same counterexample
                      // that keeps push_down_limit_through_join disabled.
                      if (memo.Get(pushed_id).logical_properties.IsNotNull(
                              column.Name().ToString()) ||
                          memo.Get(pushed_id).logical_properties.IsNotNull(
                              column.Name().name)) {
                        proven = true;
                      }
                      break;
                    }
                  }
                  if (proven) {
                    break;
                  }
                }
              }
              if (!proven) {
                continue;
              }
              // All reads done; memo mutations below use owned copies only.
              // Tags carry the keys and the join predicate: two TopNs with
              // different keys, or two joins with different predicates over
              // the same relations, must never share a derived group (D1:
              // one group, one meaning).
              std::string keys_sig;
              for (const NamedExpression& key : topn_keys) {
                keys_sig.append(std::to_string(key.name.size()));
                keys_sig.push_back(':');
                keys_sig.append(key.name);
                keys_sig.push_back('=');
                keys_sig.append(key.expression ? key.expression->ToString()
                                               : std::string("<null>"));
                keys_sig.push_back('|');
              }
              keys_sig.append(std::to_string(topn_ascending.size()));
              keys_sig.push_back(':');
              for (const bool ascending : topn_ascending) {
                keys_sig.push_back(ascending ? '1' : '0');
              }
              keys_sig.push_back('/');
              keys_sig.append(std::to_string(topn_nulls_first.size()));
              keys_sig.push_back(':');
              for (const std::optional<bool>& nulls_first : topn_nulls_first) {
                keys_sig.push_back(nulls_first.has_value()
                                       ? (nulls_first.value() ? '1' : '0')
                                       : '?');
              }
              const std::vector<std::string> pushed_relations =
                  memo.Get(pushed_id).relations;
              std::string tag = "topn_proven_one_to_one:";
              tag += std::to_string(total_limit);
              tag += ":";
              tag += pushed_rel;
              tag += ":";
              tag += other_rel;
              tag += ":";
              tag += keys_sig;
              const GroupId pushed_topn_group =
                  memo.EnsureDerivedGroup(pushed_relations, tag);
              if (pushed_topn_group == pushed_id ||
                  pushed_topn_group == group) {
                continue;
              }
              memo.AddExpression(
                  pushed_topn_group,
                  LogicalExpression{.operation = LogicalOperator::kTopN,
                                    .children = {pushed_id},
                                    .target_list = topn_keys,
                                    .sort_ascending = topn_ascending,
                                    .sort_nulls_first = topn_nulls_first,
                                    .limit_count = total_limit,
                                    .limit_offset = 0});
              LogicalExpression pushed_join = join_expr;
              pushed_join.children =
                  side == 0 ? std::vector<GroupId>{pushed_topn_group, other_id}
                            : std::vector<GroupId>{other_id, pushed_topn_group};
              std::string pushed_join_tag;
              pushed_join_tag += "topn_proven_join:";
              pushed_join_tag += std::to_string(total_limit);
              pushed_join_tag += ':';
              pushed_join_tag += pushed_rel;
              pushed_join_tag += ':';
              pushed_join_tag += other_rel;
              pushed_join_tag += ':';
              pushed_join_tag += join_predicate->ToString();
              pushed_join_tag += ':';
              pushed_join_tag += keys_sig;
              const GroupId pushed_join_group =
                  memo.EnsureDerivedGroup(input_relations, pushed_join_tag);
              if (pushed_join_group == group || pushed_join_group == input_id) {
                continue;
              }
              memo.AddExpression(pushed_join_group, std::move(pushed_join));
              memo.AddExpression(
                  group, LogicalExpression{.operation = LogicalOperator::kTopN,
                                           .children = {pushed_join_group},
                                           .target_list = topn_keys,
                                           .sort_ascending = topn_ascending,
                                           .sort_nulls_first = topn_nulls_first,
                                           .limit_count = topn_limit,
                                           .limit_offset = topn_offset});
            }
          }
        },
        LogicalOperator::kTopN));

    // or_to_union (TODO.md item 4a): Selection(OR(d0, d1)) over a
    // relation set gains a UNION ALL alternative: UnionAll(Selection(d0),
    // Selection(d1)). Each branch can then use its own index range while
    // IS TRUE partition predicates preserve the OR multiset even when the
    // branches overlap. Gates: 2..8
    // disjuncts, stable branch predicates (no subquery/aggregate/window/
    // volatile duplication across the branches), and not the same-column-
    // equality shape that or_to_in already covers.
    // Branch tags are content-addressed so different ORs never share a
    // branch group (D1: one group, one meaning).
    built.Add(Rule(
        "or_to_union", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const Expression predicate = *expression.predicate;
          const std::vector<Expression> disjuncts =
              relational_detail::SplitDisjuncts(predicate);
          if (disjuncts.size() < 2 || disjuncts.size() > 8 ||
              std::ranges::any_of(disjuncts, [](const Expression& branch) {
                return !branch;
              })) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const std::vector<std::string> input_relations =
              memo.Get(input_id).relations;
          if (input_relations.empty()) {
            return;
          }
          auto stable_branch = [&](const Expression& branch) {
            if (!branch || !ExpressionCannotThrowForPlanner(branch)) {
              return false;
            }
            std::vector<Expression> stack{branch};
            while (!stack.empty()) {
              Expression current = std::move(stack.back());
              stack.pop_back();
              if (!current) {
                continue;
              }
              switch (current->Type()) {
                case TypeTag::kQueryExp:
                case TypeTag::kAggregateExp:
                case TypeTag::kWindowFunctionExp:
                  return false;
                case TypeTag::kFunctionCallExp:
                  if (GetFunctionVolatility(
                          current->AsFunctionCallExpression().FuncName()) !=
                      Volatility::kImmutable) {
                    return false;
                  }
                  break;
                default:
                  break;
              }
              for (const ColumnName& column : current->TouchedColumns()) {
                if (!column.schema.empty() &&
                    std::ranges::find(input_relations, column.schema) ==
                        input_relations.end()) {
                  return false;
                }
              }
              for (const Expression& child : ExpressionChildren(current)) {
                if (child) {
                  stack.push_back(child);
                }
              }
            }
            return true;
          };
          if (std::ranges::any_of(disjuncts, [&](const Expression& branch) {
                return !stable_branch(branch);
              })) {
            return;
          }
          // Same-column constant equalities already fold to IN (or_to_in);
          // a union alternative would only add dedup overhead there.
          auto same_column_equality = [&](const Expression& branch,
                                          std::string* column) {
            if (!branch || branch->Type() != TypeTag::kBinaryExp) {
              return false;
            }
            const auto& binary = branch->AsBinaryExpression();
            if (binary.Op() != BinaryOperation::kEquals) {
              return false;
            }
            const Expression* column_side = nullptr;
            if (binary.Left()->Type() == TypeTag::kColumnValue &&
                binary.Right()->Type() == TypeTag::kConstantValue) {
              column_side = &binary.Left();
            } else if (binary.Right()->Type() == TypeTag::kColumnValue &&
                       binary.Left()->Type() == TypeTag::kConstantValue) {
              column_side = &binary.Right();
            } else {
              return false;
            }
            *column =
                (*column_side)->AsColumnValue().GetColumnName().ToString();
            return true;
          };
          std::string common_column;
          bool all_same_column = true;
          for (const Expression& branch : disjuncts) {
            std::string column;
            if (!same_column_equality(branch, &column)) {
              all_same_column = false;
              break;
            }
            if (common_column.empty()) {
              common_column = std::move(column);
            } else if (common_column != column) {
              all_same_column = false;
              break;
            }
          }
          if (all_same_column) {
            return;
          }
          std::vector<GroupId> branches;
          branches.reserve(disjuncts.size());
          for (size_t i = 0; i < disjuncts.size(); ++i) {
            Expression branch_predicate =
                UnaryExpressionExp(disjuncts[i], UnaryOperation::kIsTrue);
            for (size_t j = 0; j < i; ++j) {
              Expression prior_not_true = UnaryExpressionExp(
                  UnaryExpressionExp(disjuncts[j], UnaryOperation::kIsTrue),
                  UnaryOperation::kIsNotTrue);
              branch_predicate = BinaryExpressionExp(
                  std::move(branch_predicate), BinaryOperation::kAnd,
                  std::move(prior_not_true));
            }
            const GroupId branch_group = memo.EnsureDerivedGroup(
                input_relations, "or_union_branch:" + std::to_string(input_id) +
                                     ":" + std::to_string(i) + ":" +
                                     branch_predicate->ToString());
            memo.Get(branch_group).branch_local = true;
            if (branch_group == input_id || branch_group == group) {
              return;
            }
            memo.AddExpression(
                branch_group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {input_id},
                                  .predicate = std::move(branch_predicate)});
            branches.push_back(branch_group);
          }
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                       .children = std::move(branches)});
        },
        LogicalOperator::kSelection));

    // distinct_eager_over_join (TODO.md item 4b): Distinct(Join(L, R)) gains
    // a Join(Distinct(L), R) alternative when the other side's join keys are
    // unique. Each pushed-side row then matches at most one other-side row,
    // so deduplicating before the join yields the same rows while shrinking
    // the join input. No containment proof is needed (unlike TopN/limit
    // pushes): unmatched rows simply fail to join in both shapes, and for
    // LEFT joins they null-pad identically after dedup. The outer-join
    // side loop is restricted to the preserved (left) side: deduplicating
    // the null-supplying side would collapse fan-out matches. v1 scope:
    // full-row DISTINCT directly above the join (a Projection in between
    // keeps the existing path).
    built.Add(Rule(
        "distinct_eager_over_join", Distinct(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kDistinct ||
              expression.children.size() != 1) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const std::vector<LogicalExpression> join_candidates =
              memo.Get(input_id).expressions;
          const std::vector<std::string> input_relations =
              memo.Get(input_id).relations;
          for (const LogicalExpression& join_expr : join_candidates) {
            const bool is_inner = join_expr.operation == LogicalOperator::kJoin;
            const bool is_left_outer =
                join_expr.operation == LogicalOperator::kOuterJoin &&
                join_expr.join_type == 0;
            if ((!is_inner && !is_left_outer) ||
                join_expr.children.size() != 2 ||
                !join_expr.predicate.has_value() || !*join_expr.predicate) {
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            const std::vector<std::string> left_relations =
                memo.Get(left_id).relations;
            const std::vector<std::string> right_relations =
                memo.Get(right_id).relations;
            if (left_relations.size() != 1 || right_relations.size() != 1) {
              continue;
            }
            const Expression join_predicate = *join_expr.predicate;
            const int side_limit = is_inner ? 2 : 1;
            for (int side = 0; side < side_limit; ++side) {
              const GroupId pushed_id = side == 0 ? left_id : right_id;
              const GroupId other_id = side == 0 ? right_id : left_id;
              const std::string pushed_rel =
                  side == 0 ? left_relations.front() : right_relations.front();
              const std::string other_rel =
                  side == 0 ? right_relations.front() : left_relations.front();
              std::unordered_set<std::string> other_join_cols;
              for (const Expression& conjunct :
                   SplitConjuncts(join_predicate)) {
                if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
                  continue;
                }
                const auto& binary = conjunct->AsBinaryExpression();
                if (binary.Op() != BinaryOperation::kEquals ||
                    binary.Left()->Type() != TypeTag::kColumnValue ||
                    binary.Right()->Type() != TypeTag::kColumnValue) {
                  continue;
                }
                for (const Expression& key : {binary.Left(), binary.Right()}) {
                  const ColumnName& column =
                      key->AsColumnValue().GetColumnName();
                  if (column.schema == other_rel) {
                    other_join_cols.insert(column.ToString());
                    other_join_cols.insert(column.name);
                  }
                }
              }
              if (other_join_cols.empty() ||
                  !memo.Get(other_id).logical_properties.IsUniqueOn(
                      other_join_cols)) {
                continue;
              }
              // Tags omit the predicate for the pushed Distinct (its meaning
              // depends only on the pushed side) but carry it for the pushed
              // join: different predicates over the same relations must
              // never share a join group (D1).
              const std::vector<std::string> pushed_relations =
                  memo.Get(pushed_id).relations;
              const GroupId pushed_distinct = memo.EnsureDerivedGroup(
                  pushed_relations, "distinct_eager:" + pushed_rel + ":" +
                                        std::to_string(pushed_id));
              if (pushed_distinct == pushed_id || pushed_distinct == group) {
                continue;
              }
              memo.AddExpression(
                  pushed_distinct,
                  LogicalExpression{.operation = LogicalOperator::kDistinct,
                                    .children = {pushed_id}});
              LogicalExpression pushed_join = join_expr;
              pushed_join.children =
                  side == 0 ? std::vector<GroupId>{pushed_distinct, other_id}
                            : std::vector<GroupId>{other_id, pushed_distinct};
              const GroupId pushed_join_group = memo.EnsureDerivedGroup(
                  input_relations, "distinct_eager_join:" + pushed_rel + ":" +
                                       join_predicate->ToString());
              if (pushed_join_group == group || pushed_join_group == input_id) {
                continue;
              }
              memo.AddExpression(pushed_join_group, std::move(pushed_join));
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kDistinct,
                                    .children = {pushed_join_group}});
            }
          }
        },
        LogicalOperator::kDistinct));

    // topn_push_through_union_all (TODO.md item 4c): TopN(keys, limit,
    // offset) over UnionAll gains a UnionAll(TopN(keys, limit+offset)...)
    // alternative keeping the parent TopN. Every branch's top-(limit+offset)
    // contains all its rows that can appear in the global top, so the
    // parent re-establishes the exact output (D6: the parent TopN keeps
    // establishing the order). WITH TIES is excluded (tied peers overflow
    // the pushed cap). Branch tags carry the branch GroupId, ordering
    // signature, and union fingerprint so different UNION ALLs never share a
    // capped branch group (D1).
    built.Add(Rule(
        "topn_push_through_union_all", TopN(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kTopN ||
              expression.children.size() != 1 || expression.with_ties) {
            return;
          }
          if (expression.limit_offset >
              std::numeric_limits<size_t>::max() - expression.limit_count) {
            return;
          }
          const size_t total_limit =
              expression.limit_count + expression.limit_offset;
          if (total_limit == 0 || expression.target_list.empty() ||
              expression.target_list.size() !=
                  expression.sort_ascending.size()) {
            return;
          }
          const std::vector<NamedExpression> topn_keys = expression.target_list;
          const std::vector<bool> topn_ascending = expression.sort_ascending;
          const std::vector<std::optional<bool>> topn_nulls_first =
              expression.sort_nulls_first;
          std::string topn_key_signature;
          for (const NamedExpression& key : topn_keys) {
            topn_key_signature.append(std::to_string(key.name.size()));
            topn_key_signature.push_back(':');
            topn_key_signature.append(key.name);
            topn_key_signature.push_back('=');
            topn_key_signature.append(
                key.expression ? key.expression->ToString() : "<null>");
            topn_key_signature.push_back('|');
          }
          topn_key_signature.append(std::to_string(topn_ascending.size()));
          topn_key_signature.push_back(':');
          for (const bool ascending : topn_ascending) {
            topn_key_signature.push_back(ascending ? '1' : '0');
          }
          topn_key_signature.push_back('/');
          topn_key_signature.append(std::to_string(topn_nulls_first.size()));
          topn_key_signature.push_back(':');
          for (const std::optional<bool>& nulls_first : topn_nulls_first) {
            topn_key_signature.push_back(nulls_first.has_value()
                                             ? (nulls_first.value() ? '1' : '0')
                                             : '?');
          }
          const size_t topn_limit = expression.limit_count;
          const size_t topn_offset = expression.limit_offset;
          auto keys_match = [&](const LogicalExpression& candidate) {
            if (candidate.target_list.size() != topn_keys.size()) {
              return false;
            }
            for (size_t i = 0; i < topn_keys.size(); ++i) {
              if (candidate.target_list[i].name != topn_keys[i].name) {
                return false;
              }
              const std::string left = topn_keys[i].expression
                                           ? topn_keys[i].expression->ToString()
                                           : std::string("<null>");
              const std::string right =
                  candidate.target_list[i].expression
                      ? candidate.target_list[i].expression->ToString()
                      : std::string("<null>");
              if (left != right) {
                return false;
              }
            }
            return candidate.sort_ascending == topn_ascending &&
                   candidate.sort_nulls_first == topn_nulls_first;
          };
          const GroupId input_id = bindings.at("input");
          const std::vector<LogicalExpression> union_candidates =
              memo.Get(input_id).expressions;
          const std::vector<std::string> input_relations =
              memo.Get(input_id).relations;
          for (const LogicalExpression& union_expr : union_candidates) {
            if (union_expr.operation != LogicalOperator::kUnionAll ||
                union_expr.children.size() < 2) {
              continue;
            }
            std::vector<GroupId> capped_children;
            capped_children.reserve(union_expr.children.size());
            bool skip = false;
            for (const GroupId child_id : union_expr.children) {
              for (const LogicalExpression& existing :
                   memo.Get(child_id).expressions) {
                if (existing.operation == LogicalOperator::kTopN &&
                    existing.limit_offset == 0 &&
                    existing.limit_count <= total_limit &&
                    keys_match(existing)) {
                  skip = true;
                  break;
                }
              }
              if (skip) {
                break;
              }
              const GroupId capped = memo.EnsureDerivedGroup(
                  memo.Get(child_id).relations,
                  "topn_union_child:" + std::to_string(child_id) + ":" +
                      std::to_string(total_limit) + ":" + topn_key_signature);
              if (capped == child_id || capped == group) {
                skip = true;
                break;
              }
              memo.AddExpression(
                  capped,
                  LogicalExpression{.operation = LogicalOperator::kTopN,
                                    .children = {child_id},
                                    .target_list = topn_keys,
                                    .sort_ascending = topn_ascending,
                                    .sort_nulls_first = topn_nulls_first,
                                    .limit_count = total_limit,
                                    .limit_offset = 0});
              capped_children.push_back(capped);
            }
            if (skip) {
              continue;
            }
            LogicalExpression capped_union = union_expr;
            capped_union.children = std::move(capped_children);
            const GroupId capped_union_group = memo.EnsureDerivedGroup(
                input_relations,
                "topn_union_pushed:" + std::to_string(total_limit) + ":" +
                    topn_key_signature + ":" + union_expr.Fingerprint());
            if (capped_union_group == group || capped_union_group == input_id) {
              continue;
            }
            memo.AddExpression(capped_union_group, std::move(capped_union));
            memo.AddExpression(
                group, LogicalExpression{.operation = LogicalOperator::kTopN,
                                         .children = {capped_union_group},
                                         .target_list = topn_keys,
                                         .sort_ascending = topn_ascending,
                                         .sort_nulls_first = topn_nulls_first,
                                         .limit_count = topn_limit,
                                         .limit_offset = topn_offset});
          }
        },
        LogicalOperator::kTopN));

    // split_window: Window computing functions over incompatible PARTITION BY
    // specs -> stacked homogeneous Windows. Each Window node carries a single
    // partition_by payload, so mixed specs are split: the inner node computes
    // one spec's functions (adding columns, keeping every row) and the outer
    // node computes the rest over the widened input. Partition keys are input
    // columns preserved by the inner node, so both levels see identical
    // partitions.
    built.Add(Rule(
        "split_window", Window(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kWindow ||
              expression.children.size() != 1 ||
              expression.target_list.size() < 2) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          std::unordered_map<std::string, std::vector<size_t>> by_spec;
          std::vector<std::string> spec_order;
          for (size_t i = 0; i < expression.target_list.size(); ++i) {
            const NamedExpression& target = expression.target_list[i];
            if (!target.expression ||
                target.expression->Type() != TypeTag::kWindowFunctionExp) {
              return;
            }
            const auto& call = static_cast<const WindowFunctionCallExpression&>(
                *target.expression);
            std::string spec;
            for (const Expression& part : call.partition_by) {
              spec.append(part ? part->ToString() : "<null>");
              spec.push_back(';');
            }
            if (!by_spec.contains(spec)) {
              spec_order.push_back(spec);
            }
            by_spec[spec].push_back(i);
          }
          if (spec_order.size() < 2) {
            return;
          }
          const std::vector<std::string>& relations =
              memo.Get(input_id).relations;
          GroupId current = input_id;
          for (size_t s = 0; s < spec_order.size(); ++s) {
            const bool outermost = (s + 1 == spec_order.size());
            std::vector<NamedExpression> level_targets;
            std::vector<Expression> level_partition;
            for (size_t index : by_spec[spec_order[s]]) {
              level_targets.push_back(expression.target_list[index]);
              if (level_partition.empty()) {
                const auto& call =
                    static_cast<const WindowFunctionCallExpression&>(
                        *expression.target_list[index].expression);
                level_partition = call.partition_by;
              }
            }
            if (outermost) {
              LogicalExpression outer = expression;
              outer.children = {current};
              outer.target_list = std::move(level_targets);
              outer.partition_by = std::move(level_partition);
              memo.AddExpression(group, std::move(outer));
            } else {
              std::string signature = "window-split:" + GroupIdsTag({current});
              for (const NamedExpression& target : level_targets) {
                signature.append(target.name);
                signature.push_back('=');
                signature.append(target.expression->ToString());
                signature.push_back('|');
              }
              const GroupId level =
                  memo.EnsureDerivedGroup(relations, signature);
              if (level == group || level == current) {
                return;
              }
              memo.AddExpression(
                  level, LogicalExpression{
                             .operation = LogicalOperator::kWindow,
                             .children = {current},
                             .target_list = std::move(level_targets),
                             .partition_by = std::move(level_partition)});
              current = level;
            }
          }
        },
        LogicalOperator::kWindow));

    // values_fold_into_union: UnionAll over single-row Values branches -> a
    // single Values node concatenating every branch row. UNION ALL is bag
    // concatenation, and equal-width branches share the set operation's
    // positional column contract, so the fold preserves the multiset -- but
    // a real UNION ALL also coerces every branch to the set-op result type,
    // while ValuesExecutor emits rows verbatim. Every branch column type
    // must therefore match the first branch's declared type, or the folded
    // node would claim INT64 while carrying a DOUBLE value
    // (`SELECT 1 UNION ALL SELECT 2.5`). Only single-row branches fold:
    // collapsing multi-row branches would destroy the merge/parallelism
    // alternatives (MergeAppend over individually ordered branches) that
    // costing otherwise exploits.
    built.Add(Rule(
        "values_fold_into_union", UnionAll(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kUnionAll ||
              expression.children.size() < 2) {
            return;
          }
          std::vector<Row> folded;
          size_t width = 0;
          Schema folded_schema;
          bool first = true;
          for (GroupId child : expression.children) {
            bool found_values = false;
            for (const LogicalExpression& branch :
                 memo.Get(child).expressions) {
              if (branch.operation != LogicalOperator::kValues) {
                continue;
              }
              if (first) {
                width = branch.output_schema.ColumnCount();
                folded_schema = branch.output_schema;
                first = false;
              }
              if (branch.output_schema.ColumnCount() != width ||
                  branch.values.size() != 1) {
                return;
              }
              for (size_t col = 0; col < width; ++col) {
                if (branch.output_schema.GetColumn(col).Type() !=
                    folded_schema.GetColumn(col).Type()) {
                  // Set-op type promotion would coerce this branch; folding
                  // it verbatim under the first branch's schema would not.
                  return;
                }
              }
              found_values = true;
              for (const Row& row : branch.values) {
                folded.push_back(row);
              }
              break;
            }
            if (!found_values) {
              return;
            }
          }
          LogicalExpression merged = expression;
          merged.operation = LogicalOperator::kValues;
          merged.children = {};
          merged.values = std::move(folded);
          merged.output_schema = std::move(folded_schema);
          memo.AddExpression(group, std::move(merged));
        },
        LogicalOperator::kUnionAll));

    // cast_pushdown_on_comparison: CAST(col AS T) <op> const ->
    // col <op> const' when the cast is a provably injective, order-preserving,
    // total map over col's catalog domain. The rewritten column side is
    // sargable (index range extraction sees a bare column), while the
    // original is not. Soundness rests on three gates: (1) an allowlist of
    // domain pairs (same-domain redundancy, DATE -> TIMESTAMP), so lossy maps
    // such as INT64 -> FLOAT64 never rewrite; (2) a constant round-trip
    // proof CAST_BACK(const) folds and CAST_FWD(folded) reproduces const, so
    // the boundary value survives the domain change exactly; (3) totality of
    // the allowlisted maps, so no row can throw in the original and vanish
    // in the rewrite. Only single-relation selections are rewritten; joins
    // keep their conjuncts for NewJoin canonicalization.
    built.Add(Rule(
        "cast_pushdown_on_comparison", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const Group& input_group = memo.Get(input_id);
          if (input_group.relations.size() != 1) {
            return;
          }
          const std::string& relation = input_group.relations.front();
          const auto& schemas = memo.GetTableSchemas();
          const auto schema_found = schemas.find(relation);
          if (schema_found == schemas.end()) {
            return;
          }
          const Schema& schema = schema_found->second;
          bool changed = false;
          std::vector<Expression> rewritten_conjuncts;
          for (const Expression& conjunct :
               SplitConjuncts(*expression.predicate)) {
            Expression rewritten = conjunct;
            if (conjunct && conjunct->Type() == TypeTag::kBinaryExp) {
              const auto& binary = conjunct->AsBinaryExpression();
              const BinaryOperation op = binary.Op();
              const bool comparable = op == BinaryOperation::kEquals ||
                                      op == BinaryOperation::kNotEquals ||
                                      op == BinaryOperation::kLessThan ||
                                      op == BinaryOperation::kLessThanEquals ||
                                      op == BinaryOperation::kGreaterThan ||
                                      op == BinaryOperation::kGreaterThanEquals;
              const Expression* cast_side = nullptr;
              const Expression* const_side = nullptr;
              bool cast_on_left = false;
              if (binary.Left()->Type() == TypeTag::kCastExp &&
                  binary.Right()->Type() == TypeTag::kConstantValue) {
                cast_side = &binary.Left();
                const_side = &binary.Right();
                cast_on_left = true;
              } else if (binary.Right()->Type() == TypeTag::kCastExp &&
                         binary.Left()->Type() == TypeTag::kConstantValue) {
                cast_side = &binary.Right();
                const_side = &binary.Left();
                cast_on_left = false;
              }
              if (comparable && cast_side != nullptr && *cast_side &&
                  *const_side) {
                const auto& cast = (*cast_side)->AsCastExpression();
                const Value constant =
                    (*const_side)->AsConstantValue().GetValue();
                if (!constant.IsNull() && cast.Child() &&
                    cast.Child()->Type() == TypeTag::kColumnValue) {
                  const ColumnName& column =
                      cast.Child()->AsColumnValue().GetColumnName();
                  if (!column.schema.empty() &&
                      (column.schema == relation ||
                       IsSameTable(column.schema, relation))) {
                    const int offset =
                        schema.Offset(ColumnName("", column.name));
                    if (offset >= 0) {
                      const ValueType source =
                          schema.GetColumn(static_cast<size_t>(offset)).Type();
                      std::string upper;
                      for (char c : cast.TargetTypeName()) {
                        upper.push_back(static_cast<char>(
                            std::toupper(static_cast<unsigned char>(c))));
                      }
                      const bool redundant =
                          (source == ValueType::kInt64 &&
                           (upper == "INT64" || upper == "INT" ||
                            upper == "INTEGER")) ||
                          (source == ValueType::kVarChar &&
                           (upper == "STRING" || upper == "VARCHAR" ||
                            upper == "TEXT")) ||
                          (source == ValueType::kDouble &&
                           (upper == "FLOAT64" || upper == "DOUBLE" ||
                            upper == "FLOAT")) ||
                          (source == ValueType::kDate && upper == "DATE");
                      const bool date_to_timestamp =
                          source == ValueType::kDate &&
                          (upper == "TIMESTAMP" || upper == "DATETIME");
                      if (redundant || date_to_timestamp) {
                        std::string_view back_name =
                            source == ValueType::kInt64     ? "INT64"
                            : source == ValueType::kVarChar ? "STRING"
                            : source == ValueType::kDouble  ? "FLOAT64"
                                                            : "DATE";
                        const Expression back = CastExpressionExp(
                            *const_side, std::string(back_name),
                            cast.ReturnNullOnError());
                        const StatusOr<Value> folded =
                            back->TryEvaluate(Row(), Schema());
                        if (folded.HasValue() && !folded.Value().IsNull()) {
                          const Expression round_trip = CastExpressionExp(
                              ConstantValueExp(folded.Value()),
                              cast.TargetTypeName(), cast.ReturnNullOnError());
                          const StatusOr<Value> round_folded =
                              round_trip->TryEvaluate(Row(), Schema());
                          if (round_folded.HasValue() &&
                              round_folded.Value() == constant) {
                            const Expression narrowed =
                                ConstantValueExp(folded.Value());
                            rewritten = cast_on_left
                                            ? BinaryExpressionExp(cast.Child(),
                                                                  op, narrowed)
                                            : BinaryExpressionExp(narrowed, op,
                                                                  cast.Child());
                            changed = true;
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            rewritten_conjuncts.push_back(std::move(rewritten));
          }
          if (!changed) {
            return;
          }
          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kSelection,
                                .children = {input_id},
                                .predicate = CanonicalizeConjuncts(
                                    CombineConjuncts(rewritten_conjuncts))});
        },
        LogicalOperator::kSelection));

    // extract_year_sargable: EXTRACT(YEAR FROM date_col) <op> year ->
    // half-open date range on date_col, added to the scan filter while the
    // original predicate stays as the residual.  The residual re-check alone
    // does NOT make this safe: for single-relation groups the scan filter is
    // the only predicate evaluated (there is no root SelectionPlan wrap), so
    // the bounds must be semantically exact in both operand orders (see the
    // bound table in the switch below). DATE columns only: timestamps carry
    // time-of-day and time-zone behavior the range cannot reproduce. The
    // sargable range unlocks index range scans for year-filtered queries.
    built.Add(Rule(
        "extract_year_sargable", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!SelectionMayStrengthenSharedScans(memo, group)) {
            return;
          }
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const auto& schemas = memo.GetTableSchemas();
          if (schemas.empty()) {
            return;
          }
          for (const Expression& conjunct :
               SplitConjuncts(*expression.predicate)) {
            if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
              continue;
            }
            const auto& binary = conjunct->AsBinaryExpression();
            const Expression* call_side = nullptr;
            const Expression* const_side = nullptr;
            bool call_on_left = false;
            if (binary.Left()->Type() == TypeTag::kFunctionCallExp &&
                binary.Right()->Type() == TypeTag::kConstantValue) {
              call_side = &binary.Left();
              const_side = &binary.Right();
              call_on_left = true;
            } else if (binary.Right()->Type() == TypeTag::kFunctionCallExp &&
                       binary.Left()->Type() == TypeTag::kConstantValue) {
              call_side = &binary.Right();
              const_side = &binary.Left();
              call_on_left = false;
            } else {
              continue;
            }
            const auto& call = (*call_side)->AsFunctionCallExpression();
            if (call.FuncName() != "extract_year" || call.Args().size() != 1 ||
                !call.Args()[0] ||
                call.Args()[0]->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const Value year_value =
                (*const_side)->AsConstantValue().GetValue();
            if (year_value.IsNull() || year_value.type != ValueType::kInt64) {
              continue;
            }
            const int64_t year = year_value.value.int_value;
            if (year < 1 || year > 9998) {
              continue;
            }
            const ColumnName& column =
                call.Args()[0]->AsColumnValue().GetColumnName();
            if (column.schema.empty()) {
              continue;
            }
            const auto schema_found = schemas.find(column.schema);
            if (schema_found == schemas.end()) {
              continue;
            }
            const int offset =
                schema_found->second.Offset(ColumnName("", column.name));
            if (offset < 0 ||
                schema_found->second.GetColumn(static_cast<size_t>(offset))
                        .Type() != ValueType::kDate) {
              continue;
            }
            const StatusOr<Value> lower_status =
                Value::TryDate(std::to_string(year) + "-01-01");
            const StatusOr<Value> upper_status =
                Value::TryDate(std::to_string(year + 1) + "-01-01");
            if (!lower_status.HasValue() || !upper_status.HasValue()) {
              continue;
            }
            const Expression col = ColumnValueExp(column);
            const Expression lower = ConstantValueExp(lower_status.Value());
            const Expression upper = ConstantValueExp(upper_status.Value());
            std::vector<Expression> bounds;
            // Bound direction table (lower = Jan1(year), upper = Jan1(year+1)):
            //   extract <  year: col <  lower   | year <  extract: col >= upper
            //   extract <= year: col <  upper   | year <= extract: col >= lower
            //   extract >  year: col >= upper   | year >  extract: col <  lower
            //   extract >= year: col >= lower   | year >= extract: col <  upper
            // The mirrored forms swap the operands AND flip the bound; merely
            // mirroring the expression kept the old (inverted) comparison.
            switch (binary.Op()) {
              case BinaryOperation::kEquals:
                bounds.push_back(BinaryExpressionExp(
                    col, BinaryOperation::kGreaterThanEquals, lower));
                bounds.push_back(BinaryExpressionExp(
                    col, BinaryOperation::kLessThan, upper));
                break;
              case BinaryOperation::kLessThan:
                bounds.push_back(
                    call_on_left
                        ? BinaryExpressionExp(col, BinaryOperation::kLessThan,
                                              lower)
                        : BinaryExpressionExp(
                              col, BinaryOperation::kGreaterThanEquals, upper));
                break;
              case BinaryOperation::kLessThanEquals:
                bounds.push_back(
                    call_on_left
                        ? BinaryExpressionExp(col, BinaryOperation::kLessThan,
                                              upper)
                        : BinaryExpressionExp(
                              col, BinaryOperation::kGreaterThanEquals, lower));
                break;
              case BinaryOperation::kGreaterThan:
                bounds.push_back(
                    call_on_left
                        ? BinaryExpressionExp(
                              col, BinaryOperation::kGreaterThanEquals, upper)
                        : BinaryExpressionExp(col, BinaryOperation::kLessThan,
                                              lower));
                break;
              case BinaryOperation::kGreaterThanEquals:
                bounds.push_back(
                    call_on_left
                        ? BinaryExpressionExp(
                              col, BinaryOperation::kGreaterThanEquals, lower)
                        : BinaryExpressionExp(col, BinaryOperation::kLessThan,
                                              upper));
                break;
              default:
                continue;
            }
            memo.MergeScanFilter(memo.EnsureGroup({column.schema}),
                                 CombineConjuncts(bounds));
          }
        },
        LogicalOperator::kSelection));

    // comparison_self_predicates: x = x -> x IS NOT NULL; x < x / x > x ->
    // NULL-preserving FALSE; x <= x / x >= x -> NULL-preserving TRUE.
    // Equality is reflexive exactly for non-null values, so x = x is
    // equivalent to the null test; strict self-inequalities can never hold
    // but must stay NULL on null input (a bare FALSE would differ), hence
    // the CASE form. NaN breaks reflexivity, so floating-point columns are
    // excluded via the catalog domain gate (integers, dates and strings
    // only).
    built.Add(Rule(
        "comparison_self_predicates", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const auto& schemas = memo.GetTableSchemas();
          if (schemas.empty()) {
            return;
          }
          bool changed = false;
          std::vector<Expression> rewritten_conjuncts;
          for (const Expression& conjunct :
               SplitConjuncts(*expression.predicate)) {
            Expression rewritten = conjunct;
            if (conjunct && conjunct->Type() == TypeTag::kBinaryExp) {
              const auto& binary = conjunct->AsBinaryExpression();
              if ((binary.Op() == BinaryOperation::kEquals ||
                   binary.Op() == BinaryOperation::kLessThan ||
                   binary.Op() == BinaryOperation::kGreaterThan ||
                   binary.Op() == BinaryOperation::kLessThanEquals ||
                   binary.Op() == BinaryOperation::kGreaterThanEquals) &&
                  binary.Left()->Type() == TypeTag::kColumnValue &&
                  binary.Right()->Type() == TypeTag::kColumnValue &&
                  binary.Left()->AsColumnValue().GetColumnName() ==
                      binary.Right()->AsColumnValue().GetColumnName()) {
                const ColumnName& column =
                    binary.Left()->AsColumnValue().GetColumnName();
                if (!column.schema.empty()) {
                  const auto schema_found = schemas.find(column.schema);
                  if (schema_found != schemas.end()) {
                    const int offset = schema_found->second.Offset(
                        ColumnName("", column.name));
                    if (offset >= 0) {
                      const ValueType domain =
                          schema_found->second
                              .GetColumn(static_cast<size_t>(offset))
                              .Type();
                      const bool reflexive = domain == ValueType::kInt64 ||
                                             domain == ValueType::kDate ||
                                             domain == ValueType::kVarChar;
                      if (reflexive) {
                        const Expression col = ColumnValueExp(column);
                        if (binary.Op() == BinaryOperation::kEquals) {
                          rewritten = UnaryExpressionExp(
                              col, UnaryOperation::kIsNotNull);
                        } else {
                          const bool always_true =
                              binary.Op() == BinaryOperation::kLessThanEquals ||
                              binary.Op() ==
                                  BinaryOperation::kGreaterThanEquals;
                          rewritten = CaseExpressionExp(
                              {{UnaryExpressionExp(col,
                                                   UnaryOperation::kIsNull),
                                ConstantValueExp(Value())}},
                              ConstantValueExp(Value(always_true)));
                        }
                        changed = true;
                      }
                    }
                  }
                }
              }
            }
            rewritten_conjuncts.push_back(std::move(rewritten));
          }
          if (!changed) {
            return;
          }
          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kSelection,
                                .children = {input_id},
                                .predicate = CanonicalizeConjuncts(
                                    CombineConjuncts(rewritten_conjuncts))});
        },
        LogicalOperator::kSelection));

    return built;
  }();
  return rules;
}

}  // namespace tinylamb::cascades
