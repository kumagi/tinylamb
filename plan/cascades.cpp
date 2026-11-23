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
#include <cstdlib>
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
#include "common/log_message.hpp"
#include "common/string_util.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
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

// LogicalProperties matchers (IsUniqueOn / IsNotNull / AreEqual) and the
// exact `unordered_set<std::string>::contains` probes in the join rules
// accept BOTH spellings of a column -- the qualified "schema.name" and
// the bare "name" -- so rule-side sets keep both forms. A schema-less
// ColumnName has ToString() == name; the second insert would be a
// duplicate lookup, so only the bare name goes in.
inline void InsertColumnMatcherNames(std::unordered_set<std::string>& out,
                                     const ColumnName& col) {
  if (col.schema.empty()) {
    out.insert(col.name);
    return;
  }
  out.insert(col.ToString());
  out.insert(col.name);
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
// Content-addressed child identity for derived-group tags (D1: one group,
// one meaning). Without it two nodes with the same target list can share a
// derived group even though their children carry different rows.
std::string ChildGroupIdsFingerprint(const std::vector<GroupId>& children) {
  std::string out;
  out.reserve(children.size() * 21);
  for (GroupId child : children) {
    AppendInteger(out, child);
    out.push_back(',');
  }
  out.push_back('|');
  return out;
}

std::string TargetListFingerprint(const std::vector<NamedExpression>& targets) {
  std::string out;
  out.reserve(targets.size() * 32);
  // Length-prefix each component: quoted identifiers / string literals can
  // contain '=' or '|', so raw concatenation can collide two distinct
  // target lists into one derived-group tag.
  for (const NamedExpression& target : targets) {
    const std::string expression =
        target.expression ? target.expression->ToString() : "<null>";
    AppendInteger(out, target.name.size());
    out.push_back(':');
    out += target.name;
    AppendInteger(out, expression.size());
    out.push_back(':');
    out += expression;
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
  out.reserve(1 + (ids.size() * 21));
  for (const GroupId id : ids) {
    AppendInteger(out, id);
    out.push_back(',');
  }
  return out;
}

std::string GroupIdsTag(const std::vector<GroupId>& ids) {
  std::string out = "@";
  out.reserve(1 + (ids.size() * 21));
  for (const GroupId id : ids) {
    AppendInteger(out, id);
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
  preds.reserve(conjuncts.size());
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
    } else if (c->Type() == TypeTag::kBinaryExp && !ContainsParameterSlot(c)) {
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
  // `col = NULL` is UNKNOWN, not FALSE: treating it as a proven contradiction
  // is semantically wrong (e.g. under OR/NOT or in an ON clause). The
  // existing equality/null pair checks already handle the true contradictions.
  for (size_t i = 0; i < preds.size(); ++i) {
    for (size_t j = i + 1; j < preds.size(); ++j) {
      if (preds[i].col_name != preds[j].col_name) {
        continue;
      }
      // col IS NULL AND col IS NOT NULL -> a value cannot be both. The pair
      // checks below require a non-null-test operand, so this shape was
      // otherwise unreachable (and fell through the continue at the end).
      if (preds[i].is_null_test && preds[j].is_null_test &&
          preds[i].is_null_positive != preds[j].is_null_positive) {
        return true;
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
      // DATE is stored as signed day counts, so the same signed range
      // reasoning applies; DOUBLE uses its own ordered domain.
      if (preds[i].val.type == preds[j].val.type &&
          (preds[i].val.type == ValueType::kInt64 ||
           preds[i].val.type == ValueType::kDate ||
           preds[i].val.type == ValueType::kDouble)) {
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
        if (preds[i].val.type == ValueType::kDouble) {
          contradiction = EmptyRangeOrEqualityContradiction(
              oi, preds[i].val.value.double_value, oj,
              preds[j].val.value.double_value);
        } else if (preds[i].val.IsUnsigned()) {
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
  if (!expression || ContainsParameterSlot(expression)) {
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

// Pushing a single-relation conjunct into a shared scan group is only sound
// when every join alternative over that group is an inner join. A memo group
// can hold both kJoin and kOuterJoin alternatives (RIGHT is normalized to a
// swapped LEFT, FULL is decomposed), and an outer join NULL-supplies one side:
// filtering that side's scan before the join would remove padded rows.
bool HasOuterJoinAlternative(const Memo& memo, GroupId group) {
  return std::ranges::any_of(
      memo.Get(group).expressions, [](const LogicalExpression& alternative) {
        return alternative.operation == LogicalOperator::kOuterJoin;
      });
}

// Extract per-table local conditions from an OR predicate that touches
// multiple tables.  For each table involved, collect the conjuncts from every
// OR branch that reference only that table.  If every branch has at least one
// local conjunct for a given table, the local disjunction is pushed to that
// table's scan filter as a selective pre-filter.  Null-rejection is not
// analyzed here; callers gate this via `relation_enabled`.
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
      const auto branch_conjuncts = SplitConjuncts(branch);
      std::vector<Expression> local;
      local.reserve(branch_conjuncts.size());
      for (const Expression& conjunct : branch_conjuncts) {
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

// Pushes every single-relation conjunct of `predicate` into the matching scan
// group's filter. Returns the conjuncts that have no single-relation home and
// therefore must stay in the Selection. Guard rail (Phase 2): nothing here
// reasons about null-rejection, so the rule must never be used to push
// predicates through outer joins (kOuterJoin rules handle null-rejection
// separately).
std::vector<Expression> PushSingleRelationConjuncts(
    Memo& memo, const Expression& predicate,
    const std::function<bool(const std::string&)>& relation_enabled) {
  const auto conjuncts = SplitConjuncts(predicate);
  std::vector<Expression> residual;
  residual.reserve(conjuncts.size());
  for (const Expression& conjunct : conjuncts) {
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
  grouping.reserve(outputs.size());
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

// True when catalog schemas prove `left` and `right` have different ValueTypes.
// Cross-type numeric equality (e.g. INT64 `2^53 + 1` vs DOUBLE `2^53`) is NOT
// transitive and does not preserve order comparisons; when schemas are absent
// (synthetic memo unit tests), keep the untyped behavior.
// Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
//   (cpp_cross_type_int64_double_nontransitive_bug,
//    same_domain_infer_join_constants_and_inequalities_sound)
bool ColumnsProvenDifferentType(const Memo& memo, const ColumnName& left,
                                const ColumnName& right) {
  if (left.schema.empty() || right.schema.empty()) {
    return false;
  }
  const auto& schemas = memo.GetTableSchemas();
  const auto lf = schemas.find(left.schema);
  const auto rf = schemas.find(right.schema);
  if (lf == schemas.end() || rf == schemas.end()) {
    return false;
  }
  const int lo = lf->second.Offset(ColumnName("", left.name));
  const int ro = rf->second.Offset(ColumnName("", right.name));
  if (lo < 0 || ro < 0) {
    return false;
  }
  return lf->second.GetColumn(static_cast<size_t>(lo)).Type() !=
         rf->second.GetColumn(static_cast<size_t>(ro)).Type();
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
    if (ColumnsProvenDifferentType(memo, left, right)) {
      continue;
    }
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

  // Resolve set-op output column `i` to the source column inside branch `g`.
  // The set-op output name may be an alias (`SELECT v AS w`) and, in a
  // multi-relation branch, `relations.front()` need not be the relation that
  // owns the column. Recover the real source from the branch's target list;
  // only fall back to the output name (qualified by the sole relation) when
  // no target list is available. A multi-relation branch with a bare column
  // has no provable owner, so the caller must decline the rewrite.
  // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
  //   (repaired_intersect_to_semijoin_sound, repaired_except_to_antijoin_sound,
  //    cpp_intersect_except_nan_isNotDistinctFrom_bug,
  //    cpp_intersect_computed_alias_bug)
  // Note: SetOperationExecutor treats NaN == NaN as equal in INTERSECT/EXCEPT,
  // whereas BinaryOperation::kIsNotDistinctFrom delegates non-null equality to
  // `=` (where NaN = NaN is FALSE). Exclude DOUBLE/ARRAY columns accordingly.
  const auto is_nan_free_setop_domain = [](ValueType t) {
    return t == ValueType::kInt64 || t == ValueType::kVarChar ||
           t == ValueType::kDate;
  };
  const auto column_domain_safe = [&](const ColumnName& col,
                                      std::optional<ValueType> known_type) {
    if (known_type.has_value() && *known_type != ValueType::kNull) {
      return is_nan_free_setop_domain(*known_type);
    }
    const auto& schemas = memo.GetTableSchemas();
    if (schemas.empty()) {
      // No catalog information means the column type is unknown: a DOUBLE
      // (including a NaN produced without any table, e.g. 0.0/0.0) could be
      // present, so this must fail closed like the missing-schema cases below.
      return false;
    }
    if (col.schema.empty()) {
      return false;
    }
    const auto found = schemas.find(col.schema);
    if (found == schemas.end()) {
      return false;
    }
    const int offset = found->second.Offset(ColumnName("", col.name));
    if (offset < 0) {
      return false;
    }
    return is_nan_free_setop_domain(
        found->second.GetColumn(static_cast<size_t>(offset)).Type());
  };

  const auto resolve_column =
      [](const Group& g, size_t i, const ColumnName& name,
         const std::string& rel) -> std::optional<ColumnName> {
    ColumnName col = name;
    for (const auto& expr : g.expressions) {
      if (expr.target_list.empty() || i >= expr.target_list.size()) {
        continue;
      }
      const NamedExpression& target = expr.target_list[i];
      if (!target.expression ||
          target.expression->Type() != TypeTag::kColumnValue) {
        // A computed branch expression (`SELECT a + 10 AS a`) is NOT equal to
        // the base table column `t1.a`; decline so SetOperationPlan executes.
        return std::nullopt;
      }
      col = target.expression->AsColumnValue().GetColumnName();
      break;
    }
    if (col.schema.empty() && g.relations.size() <= 1 && !rel.empty()) {
      col.schema = rel;
    }
    if (col.schema.empty() && g.relations.size() > 1) {
      return std::nullopt;
    }
    return col;
  };

  std::vector<Expression> conjuncts;
  conjuncts.reserve(std::max<size_t>(expression.output_schema.ColumnCount(),
                                     expression.target_list.size()));

  if (expression.output_schema.ColumnCount() > 0) {
    for (size_t i = 0; i < expression.output_schema.ColumnCount(); ++i) {
      const Column& out_col = expression.output_schema.GetColumn(i);
      const ColumnName name = out_col.Name();
      const auto left_col = resolve_column(left_group, i, name, left_rel);
      const auto right_col = resolve_column(right_group, i, name, right_rel);
      if (!left_col || !right_col ||
          !column_domain_safe(*left_col, out_col.Type()) ||
          !column_domain_safe(*right_col, out_col.Type())) {
        return Expression{};
      }
      conjuncts.push_back(BinaryExpressionExp(
          ColumnValueExp(*left_col), kRowEquality, ColumnValueExp(*right_col)));
    }
  } else if (!expression.target_list.empty()) {
    for (size_t i = 0; i < expression.target_list.size(); ++i) {
      const ColumnName name(expression.target_list[i].name);
      const auto left_col = resolve_column(left_group, i, name, left_rel);
      const auto right_col = resolve_column(right_group, i, name, right_rel);
      if (!left_col || !right_col ||
          !column_domain_safe(*left_col, std::nullopt) ||
          !column_domain_safe(*right_col, std::nullopt)) {
        return Expression{};
      }
      conjuncts.push_back(BinaryExpressionExp(
          ColumnValueExp(*left_col), kRowEquality, ColumnValueExp(*right_col)));
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
        if (!left_targets[i].expression || !right_targets[i].expression ||
            left_targets[i].expression->Type() != TypeTag::kColumnValue ||
            right_targets[i].expression->Type() != TypeTag::kColumnValue) {
          return Expression{};
        }
        const ColumnName lcol =
            left_targets[i].expression->AsColumnValue().GetColumnName();
        const ColumnName rcol =
            right_targets[i].expression->AsColumnValue().GetColumnName();
        if (!column_domain_safe(lcol, std::nullopt) ||
            !column_domain_safe(rcol, std::nullopt)) {
          return Expression{};
        }
        conjuncts.push_back(BinaryExpressionExp(left_targets[i].expression,
                                                kRowEquality,
                                                right_targets[i].expression));
      }
    } else if (left_sch.ColumnCount() > 0 &&
               left_sch.ColumnCount() == right_sch.ColumnCount()) {
      for (size_t i = 0; i < left_sch.ColumnCount(); ++i) {
        ColumnName left_col = left_sch.GetColumn(i).Name();
        ColumnName right_col = right_sch.GetColumn(i).Name();
        if (left_col.schema.empty()) {
          if (left_group.relations.size() > 1) {
            return Expression{};
          }
          if (!left_rel.empty()) {
            left_col.schema = left_rel;
          }
        }
        if (right_col.schema.empty()) {
          if (right_group.relations.size() > 1) {
            return Expression{};
          }
          if (!right_rel.empty()) {
            right_col.schema = right_rel;
          }
        }
        if (!column_domain_safe(left_col, left_sch.GetColumn(i).Type()) ||
            !column_domain_safe(right_col, right_sch.GetColumn(i).Type())) {
          return Expression{};
        }
        conjuncts.push_back(BinaryExpressionExp(
            ColumnValueExp(left_col), kRowEquality, ColumnValueExp(right_col)));
      }
    }
  }

  if (conjuncts.empty()) {
    return Expression{};
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
    if (ColumnsProvenDifferentType(memo, left, right)) {
      continue;
    }
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
        const BinaryOperation op = f_bin.Op();
        if (op != BinaryOperation::kNotEquals &&
            op != BinaryOperation::kLessThan &&
            op != BinaryOperation::kLessThanEquals &&
            op != BinaryOperation::kGreaterThan &&
            op != BinaryOperation::kGreaterThanEquals) {
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

// Fingerprints are an internal, locale-independent encoding. IdentityBuffer
// avoids stream overhead for its own payload; the enclosing Fingerprint()
// still uses an ostringstream for the full expression encoding.
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
    case TypeTag::kConstantValue: {
      // A ParameterSlot is a constant node carrying the currently bound
      // value. Encoding that value would make every fingerprint depend on the
      // prepared statement's arguments; identify the slot instead, so the
      // same prepared plan keeps one identity across rebinding.
      const auto* slot = dynamic_cast<const ParameterSlot*>(expression.get());
      if (slot != nullptr) {
        out << "param:" << slot->Index() << ';';
        break;
      }
      AppendIdentityValue(out, expression->AsConstantValue().GetValue());
      break;
    }
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

// Stable, value-independent name for a logical operator. Embedding the raw
// enum integer would rewrite every fingerprint (and every derived-group tag
// that embeds one) as soon as a new LogicalOperator value is inserted, so the
// identity stays textual like the JoinKind ledger in common/join_kind.hpp.
// Stable spelling of the legacy outer-join type (0=left, 1=right, 2=full),
// used by kOuterJoin fingerprints. Keeping it textual means re-ordering
// JoinKind or any other enum cannot rewrite an existing alternative's
// fingerprint (and therefore every derived-group tag that embeds it).
std::string OuterJoinTypeName(uint8_t join_type) {
  switch (join_type) {
    case 0:
      return "left";
    case 1:
      return "right";
    case 2:
      return "full";
    default:
      return "unknown:" + std::to_string(static_cast<unsigned>(join_type));
  }
}

std::string LogicalOperatorName(LogicalOperator op) {
  switch (op) {
    case LogicalOperator::kScan:
      return "scan";
    case LogicalOperator::kJoin:
      return "join";
    case LogicalOperator::kOuterJoin:
      return "outer_join";
    case LogicalOperator::kCrossJoin:
      return "cross_join";
    case LogicalOperator::kSemiJoin:
      return "semi_join";
    case LogicalOperator::kAntiJoin:
      return "anti_join";
    case LogicalOperator::kSelection:
      return "selection";
    case LogicalOperator::kProjection:
      return "projection";
    case LogicalOperator::kAggregation:
      return "aggregation";
    case LogicalOperator::kSort:
      return "sort";
    case LogicalOperator::kTopN:
      return "topn";
    case LogicalOperator::kDistinct:
      return "distinct";
    case LogicalOperator::kUnion:
      return "union";
    case LogicalOperator::kUnionAll:
      return "union_all";
    case LogicalOperator::kIntersect:
      return "intersect";
    case LogicalOperator::kIntersectAll:
      return "intersect_all";
    case LogicalOperator::kExcept:
      return "except";
    case LogicalOperator::kExceptAll:
      return "except_all";
    case LogicalOperator::kLimit:
      return "limit";
    case LogicalOperator::kEmpty:
      return "empty";
    case LogicalOperator::kValues:
      return "values";
    case LogicalOperator::kDummyScan:
      return "dummy_scan";
    case LogicalOperator::kWindow:
      return "window";
    case LogicalOperator::kUnnest:
      return "unnest";
    case LogicalOperator::kRecursiveCte:
      return "recursive_cte";
    case LogicalOperator::kApply:
      return "apply";
    case LogicalOperator::kSample:
      return "sample";
    case LogicalOperator::kRelational:
      return "relational";
  }
  // An unrecognised enum value must still get a distinct fingerprint:
  // collapsing every bad value to "unknown" would put semantically different
  // expressions in the same memo group ("one group, one meaning").
  return "unknown:" + std::to_string(static_cast<unsigned>(op));
}

}  // namespace

std::string LogicalExpression::Fingerprint() const {
  std::ostringstream out;
  out << LogicalOperatorName(operation) << ':' << table;
  for (GroupId child : children) {
    out << ':' << child;
  }
  if (operation == LogicalOperator::kOuterJoin) {
    out << "#j:" << OuterJoinTypeName(join_type);
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
      if (i < sort_nulls_first.size() && sort_nulls_first[i].has_value()) {
        out << (*sort_nulls_first[i] ? 'f' : 'l');
      } else {
        // Unspecified (nullopt, or a shorter vector) means the engine default,
        // which is NOT the same ordering requirement as an explicit NULLS LAST.
        out << 'd';
      }
    }
  }
  if (operation == LogicalOperator::kLimit) {
    out << "#l:" << limit_offset << ',' << limit_count;
    if (prior_limit_offset != 0) {
      out << ",prior=" << prior_limit_offset;
    }
    // WITH TIES exists on kLimit too (limit_push_through_sort forwards it);
    // without this the tied and untied variants collapse into one fingerprint.
    if (with_ties) {
      out << ",ties";
    }
  }
  if (operation == LogicalOperator::kTopN) {
    out << "#l:" << limit_offset << ',' << limit_count;
    if (prior_limit_offset != 0) {
      out << ",prior=" << prior_limit_offset;
    }
    if (with_ties) {
      out << ",ties";
    }
  }
  if (operation == LogicalOperator::kRelational) {
    // Identity must come from the statement's content, not the shared_ptr
    // address: two separately parsed/copied but equivalent statements are the
    // same alternative, and an in-place mutation must change the fingerprint.
    // Otherwise duplicates survive dedup and burn the expression cap.
    out << "#relational:";
    if (relational_statement) {
      const std::string fp = relational_statement->Fingerprint();
      out << fp.size() << ':' << fp;
    } else {
      out << "<null>";
    }
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
  // For kAggregation, AddExpression promotes a partition_by-only spelling to
  // grouping_sets and, when both are supplied, keeps partition_by merely as
  // partitioning metadata (grouping_sets is authoritative). Including both
  // lists here would split otherwise identical aggregates into different
  // fingerprints, so the metadata is deliberately left out of the key.
  const bool aggregation_has_grouping_sets =
      operation == LogicalOperator::kAggregation && !grouping_sets.empty();
  if (!partition_by.empty() && !aggregation_has_grouping_sets) {
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
    // Same content-based identity as kRelational: a pointer would vary per
    // process/allocation and defeat memo dedup and plan caching.
    out << "#apply:";
    if (relational_statement) {
      const std::string fp = relational_statement->Fingerprint();
      out << fp.size() << ':' << fp;
    } else {
      out << "<null>";
    }
    out << ':' << OuterJoinTypeName(join_type);
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
    out << ":pn=" << partition_by.size();
    for (const Expression& partition : partition_by) {
      out << ":p=";
      if (partition) {
        out << ExpressionIdentity(partition);
      }
      out << ';';
    }
  }
  return out.str();
}

std::string Memo::GroupKey(const std::vector<std::string>& relations) {
  std::string key;
  key.reserve(relations.size() * 32);
  for (const std::string& relation : relations) {
    AppendInteger(key, relation.size());
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
  const std::vector<std::string> normalized = Normalize(relations);
  if (relation_index_.empty()) {
    for (size_t i = 0; i < normalized.size(); ++i) {
      relation_index_.emplace(normalized[i], i);
    }
  } else {
    // Existing groups carry relation_mask values expressed in the bit
    // positions assigned by the first TryBuild, so a repeated build must reuse
    // that mapping. A proper subset (a small join built in a memo that already
    // knows a larger relation set) is fine as long as every relation is known;
    // requiring the *same* set would reject legitimate subset builds and fail
    // the whole query through the caller's ASSIGN_OR_RETURN.
    const bool known_relations =
        std::ranges::all_of(normalized, [this](const std::string& relation) {
          return relation_index_.contains(relation);
        });
    if (!known_relations) {
      return StatusError(StatusCode::kInvalidArgument,
                         "join graph relations differ from the existing memo");
    }
  }
  conjuncts_.clear();
  conjunct_masks_.clear();
  conjunct_root_only_.clear();
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
    // Record only in-graph endpoints here; a conjunct referencing anything
    // outside the graph is flagged root-only and excluded from every
    // connectivity/pushdown query below. Using ~0 here used to make it look
    // adjacent to every relation pair, which disabled the disconnected-join
    // fast path and pushed artificial cross products into the search.
    conjunct_masks_.push_back(mask);
    conjunct_root_only_.push_back(outside ? 1 : 0);
    conjuncts_.push_back(info.conjunct);
  }
  // Only the singleton scans in this build have a new set of predicates.
  // A subset build leaves scans outside its relation mask intact.
  const uint64_t build_mask = RelationMask(normalized);
  for (Group& group : groups_) {
    if (!group.tag.empty() || group.relations.size() != 1 ||
        (group.relation_mask & build_mask) == 0) {
      continue;
    }
    const Expression refreshed = ScanFilterFor(group);
    if ((!group.filter && !refreshed) ||
        (group.filter && refreshed &&
         group.filter->ToString() == refreshed->ToString())) {
      continue;
    }
    group.filter = refreshed;
    group.logical_properties_expression_count.reset();
    group.logical_properties_stale = true;
    ++revision_;
    touched_groups_.push_back(group.id);
    // Readers of logical_properties (IS NOT NULL / uniqueness / equivalence
    // classes) consult it directly and do not check the stale flag. Leaving
    // the flag set here would keep a verdict derived from the previous
    // conjunct set alive across repeated TryBuild calls; mirror
    // MergeScanFilter/AddExpression and rebuild immediately.
    DeriveLogicalProperties(group.id);
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
  matching.reserve(conjuncts_.size());
  for (size_t i = 0; i < conjuncts_.size(); ++i) {
    if (conjunct_root_only_[i] != 0U) {
      continue;
    }
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
  spanning.reserve(conjuncts_.size());
  for (size_t i = 0; i < conjuncts_.size(); ++i) {
    if (conjunct_root_only_[i] != 0U) {
      continue;
    }
    const uint64_t mask = conjunct_masks_[i];
    if ((mask & ~union_mask) != 0) {
      continue;
    }
    if (mask == 0) {
      continue;
    }
    // A conjunct whose relations sit entirely on one side belongs to that
    // side's scan filter, not to the join condition. This deliberately
    // includes one-relation self-equalities such as `t.a = t.a`: they keep
    // their NULL/NaN-rejecting semantics on the scan group, while claiming
    // them here would only assert a degenerate (single-name) equivalence
    // that logical-property inference ignores anyway.
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
    right.reserve(relations.size() - 1);
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
    // A filter transfer is an opportunistic rewrite, not a query-level
    // operation: aborting here would turn an unexpected group shape into a
    // process crash across a Status boundary. Skip the transfer and leave the
    // predicate on the source expression, which still enforces it.
    LOG(WARN) << "MergeScanFilter: expected a single-relation group, got "
              << target.relations.size() << " (tag=" << target.tag << ")";
    return;
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
  // Adding a predicate to the scan filter changes what this group proves
  // (IS NOT NULL / equality conjuncts); stale logical properties would keep
  // an older uniqueness or not-null verdict. Mirror AddExpression's
  // invalidation so DeriveLogicalProperties rebuilds and notifies dependents.
  target.logical_properties_expression_count.reset();
  target.logical_properties_stale = true;
  ++revision_;
  touched_groups_.push_back(group);
  DeriveLogicalProperties(group);
}

bool Memo::CutConnected(uint64_t left_mask, uint64_t within_mask) const {
  const uint64_t right_mask = within_mask & ~left_mask;
  if (left_mask == 0 || right_mask == 0) {
    return false;
  }
  for (size_t i = 0; i < conjunct_masks_.size(); ++i) {
    if (conjunct_root_only_[i] != 0U) {
      continue;
    }
    const uint64_t mask = conjunct_masks_[i];
    if ((mask & left_mask) != 0 && (mask & right_mask) != 0) {
      return true;
    }
  }
  return false;
}

bool Memo::JoinGraphDisconnected() const {
  return !JoinGraphConnected();
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
    if (i >= conjunct_masks_.size() || (conjunct_root_only_[i] != 0U) ||
        std::popcount(conjunct_masks_[i]) < 2 || !conjuncts_[i] ||
        conjuncts_[i]->Type() != TypeTag::kBinaryExp) {
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
  for (size_t i = 0; i < conjunct_masks_.size(); ++i) {
    if (conjunct_root_only_[i] != 0U) {
      continue;
    }
    const uint64_t mask = conjunct_masks_[i];
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

  auto [left, right] = relations.size() > kMaxJoinEnumerationRelations
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
  // aggregate (and may manufacture an invalid empty projection). For the
  // legacy spelling (partition_by only), that list is moved into
  // grouping_sets and partition_by becomes empty. When both spellings are
  // supplied, grouping_sets is authoritative for grouping and partition_by
  // is kept as the supplied partitioning metadata.
  if (expression.operation == LogicalOperator::kAggregation &&
      expression.grouping_sets.empty() && !expression.partition_by.empty()) {
    expression.grouping_sets = std::move(expression.partition_by);
  }
  switch (expression.operation) {
    case LogicalOperator::kScan: {
      // A relation is scanned by `table` when it IS that table, or when the
      // catalog-published schema for the relation (an alias such as t1_1)
      // names that table.  The catalog decides, never the alias spelling:
      // distinct real tables such as `orders` and `orders_1` stay distinct
      // (T224), while self_join_elimination's single scan of the shared
      // base table still belongs to the aliased join group.
      const auto scans_relation = [&](const std::string& rel) {
        if (rel == expression.table) {
          return true;
        }
        const auto found = table_schemas_.find(rel);
        return found != table_schemas_.end() &&
               found->second.Name() == expression.table;
      };
      if (expression.table.empty() || !expression.children.empty() ||
          expression.predicate ||
          !std::ranges::all_of(target.relations, scans_relation)) {
        CHECK_MSG(false, "scan does not belong to memo group");
      }
    } break;
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
      // A unary operator preserves the exact relation set. Compare as sets
      // (the two vectors may list the same aliases in a different order):
      // the old "all relations name the same physical table" escape hatch
      // let {t AS a, t AS b} and {t AS a} mix in one chain, violating
      // "one group, one meaning" for self-joins.
      const bool same_relation_set =
          child_rels.size() == target.relations.size() &&
          std::ranges::all_of(child_rels, [&](const std::string& rel) {
            return std::ranges::find(target.relations, rel) !=
                   target.relations.end();
          });
      if (!same_relation_set) {
        CHECK_MSG(false, "operator must preserve the group's relation set");
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
  // rejected duplicate must not trip the degradation flag either:
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
          // Fingerprint() starts with `operator:table:children...`, so this
          // prefix comparison cannot reject a genuine duplicate. Its payload
          // is serialized by ExpressionIdentity, which encodes column
          // qualifiers and collation, so no predicate detail is dropped
          // before the full comparison below.
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
  if (relations.size() < 3 || relations.size() > kMaxJoinEnumerationRelations ||
      relations.size() >= std::numeric_limits<uint64_t>::digits) {
    // Keep the shift below well-defined even if the enumeration ceiling is
    // raised past the width of the bit mask in a future change.
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
  // A 16-relation group walks 2^15 bipartitions. Precompute the join graph's
  // adjacency in the *local* bit space once, but only for a connected graph:
  // this is a constant-factor bit prefilter (it avoids building vectors and
  // calling the memo's full connectivity walkers for a rejected mask), not a
  // complexity change -- the local BFS below is still run per bipartition.
  // A disconnected graph never calls the prefilter, so paying the O(n^2)
  // CutConnected scan there would be pure overhead.
  std::vector<uint64_t> adjacent;
  if (connected) {
    adjacent.assign(relations.size(), 0);
    for (size_t a = 0; a < relations.size(); ++a) {
      for (size_t b = a + 1; b < relations.size(); ++b) {
        // Restricting `within` to the pair makes a conjunct with the mask
        // exactly {a,b} count as an edge too.
        if (memo.CutConnected(global_bit[a], global_bit[a] | global_bit[b])) {
          adjacent[a] |= uint64_t{1} << b;
          adjacent[b] |= uint64_t{1} << a;
        }
      }
    }
  }
  const auto locally_connected = [&adjacent](uint64_t mask) {
    if (adjacent.empty() || mask == 0) {
      return false;
    }
    uint64_t seen = mask & (~mask + 1);  // lowest set bit
    uint64_t frontier = seen;
    while (frontier != 0) {
      uint64_t next = 0;
      uint64_t bits = frontier;
      while (bits != 0) {
        const auto i = static_cast<size_t>(std::countr_zero(bits));
        next |= adjacent[i];
        bits &= bits - 1;
      }
      frontier = next & mask & ~seen;
      seen |= frontier;
    }
    return seen == mask;
  };
  for (uint64_t local = 1; local + 1 < limit; ++local) {
    if ((local & 1U) == 0) {
      continue;
    }
    if (connected && (!locally_connected(local) ||
                      !locally_connected((limit - 1) & ~local))) {
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

// The safe exception for Limit under Window (window下のLIMITの
// 安全な例外は下記条件で実装済み).  A window emits its rows in child order (the
// evaluator appends computed values per original index), so Limit(k) over a
// window keeps exactly the first k child rows.  Moving the limit below the
// window keeps those same rows but computes their values over truncated
// partitions; the values survive that when every value-determining row of a
// survivor sits at or before it in partition order (backward-only frames,
// prefix-counting ranking calls) AND truncation can only remove later rows (the
// child emits rows in partition order).  The helpers below encode each half.

// Length-prefixed spec signature of one window call (raw separators collide
// when a key's own text contains them, mirroring the window-layer grouping in
// optimizer.cpp).  The spec covers PARTITION BY, ORDER BY, the frame and its
// exclusion; per-call NULLS modifiers live in the gate's identity instead.
//
// The frame is part of the spec. The limit-prefix rules feed this signature
// into derived-group tags; two calls sharing PARTITION BY / ORDER BY but with
// different ROWS/RANGE bounds would otherwise share one tag, landing
// semantically different windows in the same memo group ("one group, one
// meaning") even though the gate describes mixed specs as unsupported.
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
  // Frame specification: presence, unit, both bounds (kind + length-prefixed
  // offset text) and exclusion all change a window's meaning even when its
  // partition/order keys are identical. The NULLS RESPECT/IGNORE modifiers
  // are deliberately NOT part of this shared spec: one Window may legally
  // mix calls that differ only in NULLS handling, and the gate keeps them
  // apart in its per-call identity instead (see WindowPrefixGate).
  signature += "frame:";
  signature += call.has_frame ? "1:" : "0:";
  signature += std::to_string(static_cast<int>(call.frame_unit)) + ":";
  const auto append_bound = [&signature](const WindowFrameBound& bound) {
    signature += std::to_string(static_cast<int>(bound.type)) + "/";
    const std::string text =
        bound.offset ? bound.offset->ToString() : std::string("<null>");
    signature += std::to_string(text.size()) + "/" + text + ";";
  };
  append_bound(call.frame_start);
  append_bound(call.frame_end);
  signature += std::to_string(static_cast<int>(call.exclusion)) + ":";
  return signature;
}

// Does this call keep producing the same value for a row after some of its
// partition's later rows were truncated away?
bool WindowValueStableUnderTruncation(
    const WindowFunctionCallExpression& call) {
  // Function names are query identifiers: normalise case before the lookup
  // so a lower-case `lag` built by a rule (or supplied by a non-SQL producer)
  // is recognised too. The recognised set is closed, so an unexpected alias
  // cannot be promoted to stable by case folding.
  const std::string function = AsciiToUpper(call.function);
  // Ranking values are prefix counts in partition order: ROW_NUMBER numbers
  // the rows 1..m, RANK counts strictly smaller keys (+1), DENSE_RANK counts
  // key groups.  LAG likewise reads only rows at or before the current row,
  // so removing a row strictly after a survivor in partition order cannot
  // change any of them for rows the truncation keeps.
  if (function == "ROW_NUMBER" || function == "RANK" ||
      function == "DENSE_RANK" || function == "LAG") {
    return true;
  }
  // These functions ignore ROWS bounds. Even an explicitly backward frame
  // cannot make their partition-size / positional result prefix-stable.
  if (function == "NTILE" || function == "PERCENT_RANK" ||
      function == "CUME_DIST" || function == "LEAD" ||
      function == "PERCENTILE_CONT" || function == "PERCENTILE_DISC") {
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
  // The bound *type* alone is not enough: an OFFSET PRECEDING bound whose
  // offset is a bind variable (or any non-literal expression) can evaluate to
  // a negative number at execution time, turning a planned backward frame into
  // a forward one that reads rows after the truncation point.
  const auto stable_offset = [](const WindowFrameBound& bound) {
    if (bound.type != WindowFrameBoundType::kOffsetPreceding) {
      return true;
    }
    return bound.offset && bound.offset->Type() == TypeTag::kConstantValue &&
           !ContainsParameterSlot(bound.offset);
  };
  return reads_backward(call.frame_start.type) &&
         reads_backward(call.frame_end.type) &&
         stable_offset(call.frame_start) && stable_offset(call.frame_end);
}

// Gate for the two limit-prefix window rules.  Returns nullopt when the
// window node cannot take the transform at all (non-call targets or calls
// with mixed specs); otherwise the shared spec, the shared order-term count
// (one call's count decides all of them) and whether all calls keep their
// values under truncation.
struct WindowPrefixGateResult {
  std::string spec;
  // Identity of the window *outputs* (function names, args and target
  // names).  `spec` captures PARTITION/ORDER plus the frame and its
  // modifiers, so two different window calls that agree on the spec but
  // compute different values would otherwise collide on the same derived
  // group and the parent Limit could pick the wrong window output.
  std::string identity;
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
    // Length-prefix each component so distinct target sets cannot collide.
    gate.identity += std::to_string(target.name.size()) + "/" + target.name;
    // A printed form embeds the bound value of every ParameterSlot (and the
    // spelled form of literals); prepared plans rebound with other constants
    // would then land in different derived groups. ExpressionIdentity keeps
    // the identity structural and value-independent.
    const std::string expr = ExpressionIdentity(target.expression);
    gate.identity += "/" + std::to_string(expr.size()) + "/" + expr + ";";
    // NULLS RESPECT/IGNORE is per-call, not part of the shared frame spec:
    // record it here so a mixed-spec Window is still accepted while its
    // distinct outputs stay in separate derived groups.
    gate.identity += call.respect_nulls ? "/R" : "/r";
    gate.identity += call.ignore_nulls ? "/I" : "/i";
    if (!WindowValueStableUnderTruncation(call)) {
      gate.stable = false;
    }
  }
  return gate;
}

// The call-level spec/identity above does not cover the Window node's own
// output schema or its PARTITION BY list. Two Windows with identical calls can
// still publish different columns or partition rows differently, so both must
// be part of the derived-group tag (length-prefixed to avoid collisions).
std::string WindowNodeIdentity(const LogicalExpression& window) {
  std::string out = "out:";
  AppendInteger(out, window.output_schema.ColumnCount());
  for (size_t i = 0; i < window.output_schema.ColumnCount(); ++i) {
    const auto& column = window.output_schema.GetColumn(i);
    const std::string text =
        column.Name().ToString() + ':' +
        std::to_string(static_cast<int>(column.Type())) + ':' +
        std::to_string(static_cast<int>(column.GetConstraint().ctype));
    AppendInteger(out, text.size());
    out.push_back('/');
    out.append(text);
  }
  out += ":pb:";
  AppendInteger(out, window.partition_by.size());
  for (const Expression& key : window.partition_by) {
    const std::string text =
        key ? ExpressionIdentity(key) : std::string("<null>");
    AppendInteger(out, text.size());
    out.push_back('/');
    out.append(text);
  }
  return out;
}

// Any window reachable below the window's child group: the transform only
// cuts rows between the bound window and its child, so a deeper window
// feeding this one would see a truncated input without having the same
// proof made for it — refuse (the gate can learn to recurse later).
bool ReachableChildHasWindow(const Memo& memo, GroupId start) {
  // CTE expansion can build a very deep memo. Bound the walk so the
  // reachability probe cannot consume unbounded time/space; on overflow
  // answer "yes" (refuse the rewrite) rather than risk truncating a row
  // stream that a deeper window relies on.
  constexpr size_t kMaxVisitedGroups = 4096;
  std::vector<char> seen(memo.GroupCount(), 0);
  std::vector<GroupId> pending{start};
  size_t visited = 0;
  while (!pending.empty()) {
    const GroupId next = pending.back();
    pending.pop_back();
    if (static_cast<size_t>(next) >= seen.size() || (seen[next] != 0)) {
      continue;
    }
    seen[next] = 1;
    if (++visited > kMaxVisitedGroups) {
      return true;
    }
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

constexpr bool IsIdempotentDistinctAggregateType(
    AggregationType type) noexcept {
  switch (type) {
    case AggregationType::kMin:
    case AggregationType::kMax:
    case AggregationType::kLogicalAnd:
    case AggregationType::kLogicalOr:
    case AggregationType::kBitAnd:
    case AggregationType::kBitOr:
      return true;
    default:
      return false;
  }
}

bool IsMultiplicityInsensitiveAggregate(const AggregateExpression& agg) {
  if (agg.NeedsGroupContext() || !agg.InnerGroupBy().empty()) {
    return false;
  }
  if (IsIdempotentDistinctAggregateType(agg.GetType())) {
    return true;
  }
  return agg.Distinct() && agg.Child() != nullptr &&
         (agg.GetType() == AggregationType::kCount ||
          agg.GetType() == AggregationType::kSum ||
          agg.GetType() == AggregationType::kAvg);
}

Expression StripRedundantDistinctInExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, bool* changed) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    const auto& agg = expr->AsAggregateExpression();
    if (agg.Distinct() && !agg.NeedsGroupContext() &&
        IsIdempotentDistinctAggregateType(agg.GetType())) {
      *changed = true;
      return CloneAggregate(agg, /*distinct=*/false);
    }
    return expr;
  }
  if (expr->Type() == TypeTag::kWindowFunctionExp ||
      expr->Type() == TypeTag::kQueryExp) {
    return expr;
  }
  const std::vector<Expression> children = ExpressionChildren(expr);
  if (children.empty()) {
    return expr;
  }
  bool child_changed = false;
  std::vector<Expression> updated;
  updated.reserve(children.size());
  for (const Expression& child : children) {
    Expression next = StripRedundantDistinctInExpr(child, &child_changed);
    updated.push_back(std::move(next));
  }
  if (!child_changed) {
    return expr;
  }
  *changed = true;
  return WithExpressionChildren(expr, std::move(updated));
}

void CollectAggregatesAndValidate(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::vector<const AggregateExpression*>* out,
    bool* ok) {
  if (!expr || !*ok) {
    return;
  }
  if (expr->Type() == TypeTag::kWindowFunctionExp ||
      expr->Type() == TypeTag::kQueryExp) {
    *ok = false;
    return;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    out->push_back(&expr->AsAggregateExpression());
    return;
  }
  for (const Expression& child : ExpressionChildren(expr)) {
    CollectAggregatesAndValidate(child, out, ok);
  }
}

void CollectNonAggregateColumns(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::vector<ColumnName>* cols, bool* ok) {
  if (!expr || !*ok) {
    return;
  }
  if (expr->Type() == TypeTag::kWindowFunctionExp ||
      expr->Type() == TypeTag::kQueryExp) {
    *ok = false;
    return;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    return;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    cols->push_back(expr->AsColumnValue().GetColumnName());
    return;
  }
  for (const Expression& child : ExpressionChildren(expr)) {
    CollectNonAggregateColumns(child, cols, ok);
  }
}

std::optional<Expression> ResolveHavingThroughOutputs(
    const Expression& expr,
    const std::vector<NamedExpression>& outputs) {  // NOLINT(misc-no-recursion)
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    return expr;
  }
  if (expr->Type() == TypeTag::kWindowFunctionExp ||
      expr->Type() == TypeTag::kQueryExp) {
    return std::nullopt;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expr->AsColumnValue().GetColumnName();
    for (const NamedExpression& output : outputs) {
      if (OutputMatchesColumn(output, column)) {
        return output.expression;
      }
    }
    return std::nullopt;
  }
  const std::vector<Expression> children = ExpressionChildren(expr);
  if (children.empty()) {
    return expr;
  }
  std::vector<Expression> rewritten;
  rewritten.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped =
        ResolveHavingThroughOutputs(child, outputs);
    if (!mapped) {
      return std::nullopt;
    }
    rewritten.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expr, std::move(rewritten));
}

Expression SubstituteUnmatchedGroupAggregates(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    const auto& agg = expr->AsAggregateExpression();
    if (agg.GetType() == AggregationType::kCount) {
      return ConstantValueExp(Value(int64_t{0}));
    }
    return ColumnValueExp(ColumnName("__unmatched_having__", "val"));
  }
  const std::vector<Expression> children = ExpressionChildren(expr);
  if (children.empty()) {
    return expr;
  }
  std::vector<Expression> mapped;
  mapped.reserve(children.size());
  for (const Expression& child : children) {
    mapped.push_back(SubstituteUnmatchedGroupAggregates(child));
  }
  Expression rebuilt = WithExpressionChildren(expr, std::move(mapped));
  if (rebuilt && rebuilt->Type() == TypeTag::kBinaryExp) {
    const auto& bin = rebuilt->AsBinaryExpression();
    if (bin.Left() && bin.Right() &&
        bin.Left()->Type() == TypeTag::kConstantValue &&
        bin.Right()->Type() == TypeTag::kConstantValue) {
      const Value& lv = bin.Left()->AsConstantValue().GetValue();
      const Value& rv = bin.Right()->AsConstantValue().GetValue();
      if (!lv.IsNull() && !rv.IsNull() && lv.type == ValueType::kInt64 &&
          rv.type == ValueType::kInt64) {
        const int64_t l = lv.value.int_value;
        const int64_t r = rv.value.int_value;
        switch (bin.Op()) {
          case BinaryOperation::kEquals:
            return ConstantValueExp(Value(l == r));
          case BinaryOperation::kNotEquals:
            return ConstantValueExp(Value(l != r));
          case BinaryOperation::kLessThan:
            return ConstantValueExp(Value(l < r));
          case BinaryOperation::kLessThanEquals:
            return ConstantValueExp(Value(l <= r));
          case BinaryOperation::kGreaterThan:
            return ConstantValueExp(Value(l > r));
          case BinaryOperation::kGreaterThanEquals:
            return ConstantValueExp(Value(l >= r));
          default:
            break;
        }
      }
    }
  }
  return rebuilt;
}

bool HavingConjunctRejectsUnmatchedGroup(const Expression& resolved_conjunct) {
  if (!resolved_conjunct) {
    return false;
  }
  const Expression probe =
      SubstituteUnmatchedGroupAggregates(resolved_conjunct);
  if (!probe) {
    return false;
  }
  if (probe->Type() == TypeTag::kConstantValue) {
    const Value& val = probe->AsConstantValue().GetValue();
    return val.IsNull() || !val.Truthy();
  }
  return ExpressionRejectsNullsOnRelations(probe, {"__unmatched_having__"});
}

std::optional<ValueType> ResolveMemoColumnType(const Memo& memo,
                                               const ColumnName& col) {
  if (col.schema.empty() || col.name.empty() || col.name == "*") {
    return std::nullopt;
  }
  const auto& schemas = memo.GetTableSchemas();
  const auto it = schemas.find(col.schema);
  if (it == schemas.end()) {
    return std::nullopt;
  }
  const int off = it->second.Offset(ColumnName("", col.name));
  if (off < 0) {
    return std::nullopt;
  }
  return it->second.GetColumn(static_cast<size_t>(off)).Type();
}

}  // namespace

const RuleSet& RuleSet::Default() {
  static const RuleSet rules = [] {
    using namespace dsl;
    RuleSet built;
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (inner_join_comm_and_assoc_sound)
    built.Add(Rule(
        "join_enumeration", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          EnumerateJoinGroup(memo, group, expression);
        },
        LogicalOperator::kJoin));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (inner_join_comm_and_assoc_sound)
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
    // Two complementary associativity rotations. They look redundant,
    // but the worklist applies each rule once per expression occurrence, so
    // neither direction alone sees every child-group state in time; together
    // they derive every join shape. Their overlapping OUTPUTS are duplicates
    // by fingerprint, and Memo::AddExpression rejects those for free (before
    // the cap), so the pair no longer pollutes the expression budget.
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (inner_join_comm_and_assoc_sound)
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (inner_join_comm_and_assoc_sound)
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (core_join_identity_and_empty_sound)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (not_null_and_trivial_selection_sound)
    built.Add(Rule(
        "eliminate_false_selection", Selection(Any()),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate ||
              (*expression.predicate)->Type() != TypeTag::kConstantValue ||
              ContainsParameterSlot(*expression.predicate)) {
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
    // Lean proof: lean/TinylambOpt/ThreeValued.lean (and_inWhere)
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
    // implementation rules can choose the index scan plans. Guard rail: never
    // push through outer joins (null-rejection analysis does not exist yet).
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (push_and_split_selection_over_join_sound)
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (push_and_split_selection_over_join_sound)
    built.Add(Rule(
        "push_selection_through_join", Selection(Join(Any(), Any(), "input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!SelectionMayStrengthenSharedScans(memo, group) ||
              HasOuterJoinAlternative(memo, bindings.at("input"))) {
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (push_and_split_selection_over_join_sound)
    built.Add(Rule(
        "split_selection_over_join", Selection(Join(Any(), Any(), "input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!SelectionMayStrengthenSharedScans(memo, group) ||
              HasOuterJoinAlternative(memo, bindings.at("input"))) {
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (push_filter_past_setop_sound)
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
            // relation lists; an unqualified column is pushable only when it
            // resolves *unambiguously* by name in that branch — exactly one
            // of its relations publishes it (a JOIN branch need not have the
            // column in all of its relations).  When the schemas are
            // unavailable the push must refuse.
            const auto touched = (*expression.predicate)->TouchedColumns();
            const auto& schemas = memo.GetTableSchemas();
            // The published map is normally keyed by the FROM alias (which is
            // what branch.relations holds), but hand-built memos may key it by
            // the real table name. Accept either spelling so an alias such as
            // `t1_1` does not leave matches==0 and reject every push.
            const auto schema_for =
                [&](const std::string& relation) -> const Schema* {
              const auto found = schemas.find(relation);
              if (found != schemas.end()) {
                return &found->second;
              }
              for (const auto& [key, schema] : schemas) {
                if (schema.Name() == relation) {
                  return &schema;
                }
              }
              return nullptr;
            };
            auto resolves_in_branch = [&](const Group& branch,
                                          const ColumnName& column) {
              if (!column.schema.empty()) {
                // "Resolves unambiguously" must check the column itself,
                // not just that the qualifier names a relation in the
                // branch: otherwise t.x is pushed where t has no x.
                if (std::ranges::find(branch.relations, column.schema) ==
                    branch.relations.end()) {
                  return false;
                }
                const Schema* schema = schema_for(column.schema);
                return schema != nullptr &&
                       schema->Offset(ColumnName("", column.name)) >= 0;
              }
              if (schemas.empty()) {
                return false;
              }
              // Count publishing relations rather than requiring all_of:
              // in a two-relation branch {a,b} a predicate touching only a.x
              // must still be pushable when b has no x. Accepting more than
              // one match would be ambiguous, so refuse that case.
              size_t matches = 0;
              for (const auto& rel : branch.relations) {
                const Schema* schema = schema_for(rel);
                if (schema != nullptr &&
                    schema->Offset(ColumnName("", column.name)) >= 0) {
                  ++matches;
                }
              }
              return matches == 1;
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (push_filter_through_distinct_sound)
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (projection_transpose_and_merge_sound)
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
    // ambiguous names or null-rejection belongs to the analyzer rather than to
    // this conservative memo rewrite; kOuterJoin exists in the memo but this
    // rule only binds kJoin.
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (projection_transpose_and_merge_sound)
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
                result += std::to_string(columns.size()) + ':';
                for (const ColumnName& column : columns) {
                  const std::string name = column.ToString();
                  result += std::to_string(name.size()) + ':' + name;
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
            left_targets.reserve(required[0].size());
            right_targets.reserve(required[1].size());
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (projection_transpose_and_merge_sound)
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (projection_transpose_and_merge_sound)
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (push_limit_and_topn_through_projection_sound)
    built.Add(Rule(
        "push_limit_through_projection", Limit(Projection(Any(), "proj")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          // WITH TIES needs the pre-projection order and the boundary row to
          // identify peers; moving the cut below the Projection loses both.
          if (expression.with_ties) {
            return;
          }
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
                    std::to_string(expression.limit_offset) +
                    ":no-ties");
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
                                  .target_list = projection.target_list,
                                  .output_schema = projection.output_schema});
          }
        },
        LogicalOperator::kLimit));
    // TopN(Projection(R)) can sort R directly when every ordering expression
    // can be translated through the projection.  Keep the projection above
    // TopN so aliases and computed output columns remain unchanged.
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (push_limit_and_topn_through_projection_sound)
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
                std::to_string(expression.limit_offset) +
                (expression.prior_limit_offset != 0
                     ? ":p" + std::to_string(expression.prior_limit_offset)
                     : "");
            for (const NamedExpression& key : translated_keys) {
              const std::string text = key.expression->ToString();
              signature.push_back('|');
              AppendInteger(signature, text.size());
              signature.push_back(':');
              signature.append(text);
            }
            signature += "|direction:";
            for (bool ascending : expression.sort_ascending) {
              signature.push_back(ascending ? '1' : '0');
            }
            signature += "|nulls:";
            for (const auto nulls_first : expression.sort_nulls_first) {
              signature.push_back(nulls_first ? (*nulls_first ? '1' : '0')
                                              : '?');
            }
            signature += expression.with_ties ? "|ties:1" : "|ties:0";
            // The child group alone does not identify the projection: two
            // different projections over that child can translate the sort
            // keys identically yet publish different output columns. Fold
            // both the target list and the output schema into the tag so
            // their pushed TopNs never share one derived group.
            signature += "|projection:";
            for (const NamedExpression& target : projection.target_list) {
              const std::string text =
                  target.expression ? ExpressionIdentity(target.expression)
                                    : std::string("<null>");
              AppendInteger(signature, target.name.size());
              signature.push_back('/');
              signature.append(target.name);
              signature.push_back('=');
              AppendInteger(signature, text.size());
              signature.push_back(':');
              signature.append(text);
              signature.push_back(';');
            }
            signature += "|schema:";
            for (size_t i = 0; i < projection.output_schema.ColumnCount();
                 ++i) {
              const auto& column = projection.output_schema.GetColumn(i);
              const std::string text =
                  column.Name().ToString() + ":" +
                  std::to_string(static_cast<int>(column.Type()));
              AppendInteger(signature, text.size());
              signature.push_back(':');
              signature.append(text);
            }
            const GroupId pushed =
                memo.EnsureDerivedGroup(input_relations, signature);
            if (pushed == group || pushed == projection.children[0]) {
              continue;
            }
            // RewriteThroughOutputs rebuilds the key expressions through
            // WithExpressionChildren, so a COLLATE wrapper on an ORDER BY key
            // survives the translation. Collation is not a separate logical
            // field: the physical TopN rule reads it back with
            // CollationOfExpression on each translated key, and the signature
            // above length-prefixes the key text (which includes COLLATE).
            memo.AddExpression(
                pushed, LogicalExpression{
                            .operation = LogicalOperator::kTopN,
                            .children = {projection.children[0]},
                            .target_list = std::move(translated_keys),
                            .sort_ascending = expression.sort_ascending,
                            .sort_nulls_first = expression.sort_nulls_first,
                            .limit_count = expression.limit_count,
                            .limit_offset = expression.limit_offset,
                            .prior_limit_offset = expression.prior_limit_offset,
                            .with_ties = expression.with_ties});
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = {pushed},
                                  .target_list = projection.target_list,
                                  .output_schema = projection.output_schema});
          }
        },
        LogicalOperator::kTopN));
    // Limit(UNION ALL(children), offset, count) only needs the first
    // offset+count rows from each child.  Keep the parent Limit so the global
    // offset remains correct, while capping every branch independently.
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (union_all_push_limit_sound)
    built.Add(Rule(
        "union_all_push_limit", Limit(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          // Capping each UNION ALL branch at offset+count is only sound for a
          // plain LIMIT. WITH TIES needs rows beyond that cap to keep peers of
          // the boundary row, so the pushdown must be skipped entirely.
          if (expression.with_ties || expression.limit_count == 0 ||
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
    // Lean proof: lean/TinylambOpt/CppRuleAudit.lean
    //   (repaired_push_sel_through_agg_sound,
    //    cpp_push_sel_through_agg_drops_grouping_bug)
    built.Add(Rule(
        "push_selection_through_aggregation",
        Selection(Aggregation(Any(), "agg")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& aggregation :
               memo.Get(bindings.at("agg")).expressions) {
            if (aggregation.operation != LogicalOperator::kAggregation ||
                aggregation.children.empty() ||
                aggregation.grouping_sets.empty()) {
              // Never push Selection below an ungrouped scalar Aggregation:
              // scalar Aggregation always emits 1 row even on empty input, so
              // pushing a false/filtering predicate below it turns 0 surviving
              // rows into 1 phantom aggregate row.
              continue;
            }
            const GroupId input = aggregation.children[0];
            if (memo.Get(input).tag.starts_with("sel-below-agg:")) {
              continue;
            }
            std::vector<NamedExpression> grouping =
                GroupingOutputs(aggregation.target_list);
            grouping.reserve(grouping.size() +
                             aggregation.grouping_sets.size());
            for (const Expression& key : aggregation.grouping_sets) {
              if (key && key->Type() == TypeTag::kColumnValue) {
                const ColumnName& col = key->AsColumnValue().GetColumnName();
                const bool already_present = std::ranges::any_of(
                    grouping, [&](const NamedExpression& out) {
                      return OutputMatchesColumn(out, col);
                    });
                if (!already_present) {
                  grouping.emplace_back(col.name, key);
                }
              }
            }
            const auto conjuncts = SplitConjuncts(*expression.predicate);
            std::vector<Expression> pushed;
            std::vector<Expression> residual;
            pushed.reserve(conjuncts.size());
            residual.reserve(conjuncts.size());
            for (const Expression& conjunct : conjuncts) {
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
                  group, LogicalExpression{
                             .operation = LogicalOperator::kAggregation,
                             .children = {filtered},
                             .predicate = aggregation.predicate,
                             .target_list = aggregation.target_list,
                             .output_schema = aggregation.output_schema,
                             .partition_by = aggregation.partition_by,
                             .grouping_sets = aggregation.grouping_sets});
              continue;
            }
            const GroupId new_agg = memo.EnsureDerivedGroup(
                memo.Get(group).relations,
                "agg-after-sel-push:" + GroupIdsTag({filtered}) +
                    TargetListFingerprint(aggregation.target_list) + "#" +
                    CombineConjuncts(residual)->ToString());
            InheritBranchLocal(memo, new_agg, group);
            if (new_agg == group || new_agg == filtered) {
              continue;
            }
            memo.AddExpression(
                new_agg,
                LogicalExpression{.operation = LogicalOperator::kAggregation,
                                  .children = {filtered},
                                  .predicate = aggregation.predicate,
                                  .target_list = aggregation.target_list,
                                  .output_schema = aggregation.output_schema,
                                  .partition_by = aggregation.partition_by,
                                  .grouping_sets = aggregation.grouping_sets});
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (same_domain_infer_join_constants_and_inequalities_sound,
    //    cpp_cross_type_int64_double_nontransitive_bug)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean (merge_limits_sound)
    built.Add(Rule(
        "merge_limits", Limit(Limit(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& inner :
               memo.Get(bindings.at("inner")).expressions) {
            if (inner.operation != LogicalOperator::kLimit ||
                inner.children.empty() || inner.children[0] == group ||
                inner.prior_limit_offset != 0 ||
                expression.prior_limit_offset != 0) {
              continue;
            }
            // Composing offsets/counts assumes plain limits. WITH TIES on
            // either side changes which rows survive (peers of the boundary
            // row), so the merged limit must not silently drop that flag.
            if (inner.with_ties || expression.with_ties) {
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (sort_merge_and_double_sort_sound)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (not_null_and_trivial_selection_sound)
    built.Add(Rule(
        "eliminate_true_selection", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate ||
              (*expression.predicate)->Type() != TypeTag::kConstantValue ||
              ContainsParameterSlot(*expression.predicate)) {
            return;
          }
          const Value value =
              (*expression.predicate)->AsConstantValue().GetValue();
          if (value.IsNull() || !value.Truthy()) {
            return;
          }
          const GroupId input = bindings.at("input");
          const Group& input_group = memo.Get(input);
          // The copied alternatives implement against this group's filter
          // (implementation_rules reads memo.Get(group).filter), so a scan
          // filter that lived only on the input group must move with them —
          // the same contract push_selection_into_scan and
          // pk_unique_distinct_elimination follow. A non-null filter implies
          // a single-relation input, and this unary Selection preserves the
          // relation set, so the destination is single-relation as well.
          if (input != group && input_group.filter &&
              memo.Get(group).relations.size() == 1) {
            // The predicate may be branch-local (a set-op branch / OR arm).
            // Mark the destination before moving it, otherwise a later rule
            // would treat this group's filter as global and merge it into a
            // scan shared with the other branches.
            InheritBranchLocal(memo, group, input);
            memo.MergeScanFilter(group, input_group.filter);
          }
          for (const LogicalExpression& child : input_group.expressions) {
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (setop_empty_simplification_sound)
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
          // This is intentionally different from setop_empty_identity below:
          // that rule fires on an empty RIGHT branch and copies the surviving
          // left input, while this one fires on an empty LEFT branch and
          // produces Empty. They therefore describe different source states
          // even though both operate on the same relation set; keeping both
          // alternatives is deliberate and not a duplicate rule.
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (setop_identity_and_union_rules_sound)
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
            survivors.reserve(expression.children.size());
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
          survivors.reserve(expression.children.size());
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (setop_identity_and_union_rules_sound)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (setop_identity_and_union_rules_sound)
    built.Add(Rule(
        "union_all_merge", Pattern::Op(LogicalOperator::kUnionAll, {}),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          std::vector<GroupId> flattened;
          flattened.reserve(expression.children.size());
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
    // Lean proof: lean/TinylambOpt/AggJoinDiscovery.lean
    //   (leftOuterJoin_filter_left)
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
          left_relations.reserve(left_group.relations.size());
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
            kept.reserve(memo.Get(bindings.at("join")).expressions.size());
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (core_join_identity_and_empty_sound)
    built.Add(Rule(
        "join_on_false_to_empty", Join(Any("left"), Any("right")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          const Expression pred = *expression.predicate;
          if (!pred || pred->Type() != TypeTag::kConstantValue ||
              ContainsParameterSlot(pred)) {
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (sort_merge_and_double_sort_sound)
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
    // Lean proof: lean/TinylambOpt/CppRuleAudit.lean
    //   (repaired_distinct_over_group_by_nodup,
    //    cpp_distinct_over_group_by_skips_expr_key_bug)
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
            // targets include EVERY grouping key (including computed keys such
            // as `t.b + 1`; skipping non-column keys would drop DISTINCT when
            // two distinct groups share the projected plain-column prefix).
            // When both spellings are present, AddExpression makes
            // grouping_sets authoritative for the emitted rows and keeps
            // partition_by only as partitioning metadata (the aggregation
            // implementation likewise ignores it). Therefore partition_by is
            // the fallback key list only for the legacy partition_by-only
            // spelling; adding its keys here would demand columns that do not
            // contribute to row uniqueness and keep a genuinely redundant
            // DISTINCT.
            const auto& keys = agg.grouping_sets.empty() ? agg.partition_by
                                                         : agg.grouping_sets;
            if (keys.empty()) {
              continue;
            }
            const auto matches_key = [](const Expression& key,
                                        const Expression& proj) {
              if (!key || !proj) {
                return false;
              }
              if (key->Type() == TypeTag::kColumnValue &&
                  proj->Type() == TypeTag::kColumnValue) {
                const ColumnName& kc = key->AsColumnValue().GetColumnName();
                const ColumnName& pc = proj->AsColumnValue().GetColumnName();
                return kc == pc || ((kc.schema.empty() || pc.schema.empty()) &&
                                    kc.name == pc.name);
              }
              if (key.get() == proj.get()) {
                return true;
              }
              return key->ToString() == proj->ToString();
            };
            // Every grouping key must map to a DISTINCT projected target: one
            // target that happens to match two keys does NOT prove both keys
            // are projected (GROUP BY a,b with targets [a,a] would otherwise
            // collapse groups (a=1,b=1) and (a=1,b=2)). Give each key its own
            // as-yet-unused target so a single heavy-hitter column cannot be
            // counted twice.
            std::vector<bool> target_used(agg.target_list.size(), false);
            size_t seen = 0;
            for (const auto& g : keys) {
              if (!g) continue;  // NOLINT(readability-braces-around-statements)
              bool matched = false;
              for (size_t j = 0; j < agg.target_list.size(); ++j) {
                if (target_used[j]) {
                  continue;
                }
                if (matches_key(g, agg.target_list[j].expression)) {
                  target_used[j] = true;
                  matched = true;
                  ++seen;
                  break;
                }
              }
              if (!matched) {
                break;
              }
            }
            if (seen != keys.size()) {
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (repaired_push_projection_through_aggregation_sound,
    //    cpp_push_proj_through_agg_prunes_having_col_bug)
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
            // Length-prefix the names/expressions (TargetListFingerprint):
            // quoted identifiers and string literals may contain '=' or ';',
            // so the previous raw concatenation could collide two different
            // target lists into one derived group.
            std::string tag =
                "proj_below_agg:" + GroupIdsTag({agg_child}) +
                "|proj:" + TargetListFingerprint(expression.target_list) +
                "|agg:" + TargetListFingerprint(agg.target_list);
            std::unordered_set<ColumnName> needed_cols;
            const auto collect_needed = [&](const Expression& e) {
              if (!e) {
                return;
              }
              for (const auto& c : e->TouchedColumns()) {
                if (c.name != "*") {
                  needed_cols.insert(c);
                }
              }
            };
            for (const auto& g : agg.grouping_sets) {
              collect_needed(g);
            }
            // partition_by is partitioning metadata once grouping_sets is
            // present (AddExpression promotes the legacy partition_by-only
            // spelling into grouping_sets), and the aggregation
            // implementation ignores it. Carrying its columns through the
            // pushed-down projection would only widen the aggregation input.
            for (const auto& t : agg.target_list) {
              collect_needed(t.expression);
            }
            if (agg.predicate) {
              collect_needed(*agg.predicate);
              if (*agg.predicate) {
                tag += "|having=" + (*agg.predicate)->ToString();
              }
            }
            if (needed_cols.empty()) {
              continue;
            }
            std::vector<ColumnName> sorted_cols(needed_cols.begin(),
                                                needed_cols.end());
            std::ranges::sort(sorted_cols,
                              [](const ColumnName& a, const ColumnName& b) {
                                // Compare components without allocating two
                                // temporary strings per comparison.
                                if (a.schema != b.schema) {
                                  return a.schema < b.schema;
                                }
                                return a.name < b.name;
                              });
            std::vector<NamedExpression> below_targets;
            below_targets.reserve(sorted_cols.size());
            for (const auto& col : sorted_cols) {
              // Empty alias keeps the full qualified ColumnName + NOT NULL
              // constraints in ProjectionPlan::GenerateSchema() so `t1.x` and
              // `t2.x` never collide as unqualified `x`.
              below_targets.emplace_back("", ColumnValueExp(col));
            }

            const GroupId proj_below =
                memo.EnsureDerivedGroup(memo.Get(agg_child).relations, tag);
            const GroupId new_agg = memo.EnsureDerivedGroup(
                memo.Get(agg_child).relations, tag + "|agg");
            InheritBranchLocal(memo, proj_below, group);
            InheritBranchLocal(memo, new_agg, group);
            if (proj_below != agg_child && proj_below != group &&
                new_agg != agg_id && new_agg != group) {
              memo.AddExpression(
                  proj_below,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {agg_child},
                                    .target_list = std::move(below_targets)});
              memo.AddExpression(
                  new_agg, LogicalExpression{
                               .operation = LogicalOperator::kAggregation,
                               .children = {proj_below},
                               .predicate = agg.predicate,
                               .target_list = agg.target_list,
                               .output_schema = agg.output_schema,
                               // Keep exactly one grouping spelling:
                               // Memo::AddExpression treats grouping_sets
                               // as authoritative when both are present,
                               // and copying both here would re-create
                               // that ambiguous state on every rewrite.
                               .partition_by = agg.grouping_sets.empty()
                                                   ? agg.partition_by
                                                   : std::vector<Expression>{},
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (limit_push_through_sort_sound, naive_limit_below_sort_unsound)
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
            // Collation is carried by the key expressions themselves (a
            // COLLATE wrapper is part of the expression tree); the physical
            // TopN rule derives each key's collation with
            // CollationOfExpression(target_list[i].expression). Copying the
            // sort's target_list verbatim therefore keeps the ordering
            // contract intact -- there is no separate logical collation field
            // to copy here.
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
    // pushing a Limit below a Window: in general, truncating the input
    // changes partition-relative computations even though a window preserves
    // rows.
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (limit_prefix_window_sound)
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
            // This rule is the `order_terms == 0` half; the ordered rule below
            // requires `order_terms != 0`. A nullopt gate (e.g. a window whose
            // ORDER BY syntax the gate cannot interpret) makes both rules
            // decline silently -- intentionally conservative, since neither
            // rewrite is sound without a proven spec, and the original
            // ordered plan remains available in the memo.
            if (!gate.has_value() || !gate->stable || gate->order_terms != 0 ||
                memo.Get(input_id).tag.starts_with("window-prefix-limit:") ||
                ReachableChildHasWindow(memo, input_id)) {
              continue;
            }
            std::string signature = GroupIdsTag({input_id}) + gate->spec +
                                    gate->identity + WindowNodeIdentity(window);
            AppendInteger(signature, expression.limit_count);
            signature += ':';
            AppendInteger(signature, expression.limit_offset);
            // The inner cut is a PREFIX: the window must still number the
            // OFFSET rows the outer Limit later skips, so keep offset+count
            // rows from the start and let only the outer Limit apply OFFSET.
            size_t prefix_rows = 0;
            if (__builtin_add_overflow(expression.limit_count,
                                       expression.limit_offset, &prefix_rows)) {
              continue;
            }
            // Check overflow before creating a derived group: otherwise a
            // rejected conversion leaves an empty group keyed by this limit
            // signature in the memo on every attempt.
            const GroupId limited =
                memo.EnsureDerivedGroup(memo.Get(input_id).relations,
                                        "window-prefix-limit:" + signature);
            if (limited == group || limited == input_id) {
              continue;
            }
            memo.AddExpression(
                limited, LogicalExpression{.operation = LogicalOperator::kLimit,
                                           .children = {input_id},
                                           .limit_count = prefix_rows,
                                           .limit_offset = 0});
            const GroupId windowed =
                memo.EnsureDerivedGroup(memo.Get(input_id).relations,
                                        "window-prefix-window:" + signature);
            // The new window must not fold back into the source window group
            // nor into the prefix/limit group it is built on: a chained rule
            // may already have claimed this signature. Otherwise a Window
            // alternative gets added to the original group ("one group, one
            // meaning") and the search can diverge.
            if (windowed == group || windowed == window_group_id ||
                windowed == input_id || windowed == limited) {
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

    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (limit_prefix_window_sound)
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
            std::string signature =
                GroupIdsTag({window_group_id, sort_group_id}) + gate->spec +
                gate->identity + WindowNodeIdentity(window);
            // A group can hold several Sort/Window alternatives with the same
            // spec but different outputs. Mix the *selected* alternatives into
            // the tag so they never land in one derived group.
            for (const LogicalExpression* chosen : {&window, sort_node}) {
              const std::string fp = chosen->Fingerprint();
              AppendInteger(signature, fp.size());
              signature.push_back('/');
              signature.append(fp);
            }
            AppendInteger(signature, expression.limit_count);
            signature += ':';
            AppendInteger(signature, expression.limit_offset);
            // Prefix cut: keep offset+count rows; OFFSET is applied once, by
            // the outer Limit (see limit_prefix_unordered_window).
            size_t prefix_rows = 0;
            if (__builtin_add_overflow(expression.limit_count,
                                       expression.limit_offset, &prefix_rows)) {
              continue;
            }
            // Refuse overflow before creating a derived group so a failed
            // conversion does not leave an empty group behind.
            const GroupId limited =
                memo.EnsureDerivedGroup(memo.Get(window_group_id).relations,
                                        "window-prefix-limit:" + signature);
            if (limited == group || limited == window_group_id ||
                limited == sort_group_id) {
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
            // Same collision guard as the unordered rule: the rewritten
            // window must stay distinct from every group it derives from.
            if (windowed == group || windowed == window_group_id ||
                windowed == sort_group_id || windowed == limited) {
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (push_filter_through_sort_perm_sound)
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
    // requirements. X is the Sort's child group, reused verbatim as the new
    // Aggregation's input; the rewrite does not rebuild any sort node.
    // The payload (grouping sets, partition, schema) is
    // copied verbatim; dropping the grouping would turn a GROUP BY
    // aggregate into a scalar one, and order-sensitive aggregates
    // (string_agg / array_agg / WITHIN GROUP / ANY_VALUE / sketches) could
    // observe a different input order after the Sort disappears.
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (repaired_elim_sort_under_agg_sound,
    //    cpp_elim_sort_under_agg_drops_having_bug)
    built.Add(Rule(
        "eliminate_sort_under_unordered_consumer",
        Aggregation(Sort(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const auto expr_order_insensitive =
              [](const auto& self, const Expression& expr) -> bool {
            if (!expr) {
              return true;
            }
            if (expr->Type() == TypeTag::kAggregateExp) {
              const auto& aggregate = dynamic_cast<const AggregateExpression&>(
                  *expr);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
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
                  break;
                default:
                  return false;
              }
            }
            // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is
            // clearer here
            for (const Expression& child : ExpressionChildren(expr)) {
              if (!self(self, child)) {
                return false;
              }
            }
            return true;
          };
          const auto order_insensitive = [&](const LogicalExpression& agg) {
            if (agg.predicate && *agg.predicate &&
                !expr_order_insensitive(expr_order_insensitive,
                                        *agg.predicate)) {
              return false;
            }
            return std::ranges::all_of(
                agg.target_list, [&](const NamedExpression& target) {
                  return expr_order_insensitive(expr_order_insensitive,
                                                target.expression);
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
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema,
                                  .partition_by = expression.partition_by,
                                  .grouping_sets = expression.grouping_sets});
          }
        },
        LogicalOperator::kAggregation));

    // distinct_over_distinct: Distinct(Distinct(X)) -> Distinct(X)
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (distinct_over_distinct_sound)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (cross_and_empty_join_simplification_sound)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (cross_and_empty_join_simplification_sound)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (repaired_intersect_to_semijoin_sound,
    //    cpp_intersect_except_nan_isNotDistinctFrom_bug,
    //    cpp_intersect_computed_alias_bug)
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
          // Include child GroupIds: without them two set operations over
          // the same relation set would share one semijoin group (D1).
          const std::string tag =
              "intersect-semi:" +
              ChildGroupIdsFingerprint(expression.children) +
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (repaired_except_to_antijoin_sound,
    //    cpp_intersect_except_nan_isNotDistinctFrom_bug)
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
              "except-anti:" + ChildGroupIdsFingerprint(expression.children) +
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
    // Lean proof: lean/TinylambOpt/CppRuleAudit.lean
    //   (repaired_distinct_over_group_by_nodup,
    //    cpp_distinct_over_group_by_skips_expr_key_bug)
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
                     !expression.grouping_sets.empty() &&
                     !(expression.predicate && *expression.predicate)) {
            bool has_agg = false;
            for (const auto& item : expression.target_list) {
              if (ContainsAggregate(item.expression)) {
                has_agg = true;
                break;
              }
            }
            if (!has_agg) {
              // Only interchange back to Distinct(Projection(X)) when EVERY
              // grouping key (including computed keys) survives the projection
              // AND every projected target is a grouping key; otherwise
              // Distinct(Projection(X)) groups at a different granularity.
              const auto matches_key = [](const Expression& key,
                                          const Expression& proj) {
                if (!key || !proj) {
                  return false;
                }
                if (key->Type() == TypeTag::kColumnValue &&
                    proj->Type() == TypeTag::kColumnValue) {
                  const ColumnName& kc = key->AsColumnValue().GetColumnName();
                  const ColumnName& pc = proj->AsColumnValue().GetColumnName();
                  return kc == pc ||
                         ((kc.schema.empty() || pc.schema.empty()) &&
                          kc.name == pc.name);
                }
                if (key.get() == proj.get()) {
                  return true;
                }
                return key->ToString() == proj->ToString();
              };
              const bool all_grouping_projected = std::ranges::all_of(
                  expression.grouping_sets, [&](const Expression& g) {
                    return g && std::ranges::any_of(
                                    expression.target_list,
                                    [&](const NamedExpression& t) {
                                      return matches_key(g, t.expression);
                                    });
                  });
              const bool all_targets_are_keys = std::ranges::all_of(
                  expression.target_list, [&](const NamedExpression& t) {
                    return t.expression &&
                           std::ranges::any_of(expression.grouping_sets,
                                               [&](const Expression& g) {
                                                 return matches_key(
                                                     g, t.expression);
                                               });
                  });
              if (!all_grouping_projected || !all_targets_are_keys) {
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (self_join_elimination_sound, self_join_elim_nullable_unique_bug)
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
          const std::optional<std::string> unique_key = SingleUniqueKeyEquality(
              memo, expression.predicate.value_or(Expression()), r1, r2, r1);
          if (!unique_key.has_value()) {
            return;
          }
          // INNER JOIN drops NULL-key rows (they never match), but the bare
          // scan this rule installs returns them.  A nullable UNIQUE column
          // still admits duplicate NULLs, so the rewrite is only safe when
          // the key is declared NOT NULL (or PRIMARY KEY).
          if (!ColumnIsDeclaredNonNull(memo, r1, ColumnName("", *unique_key))) {
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
              // Both sides scan the same table, so every column name exists
              // on both sides. An unqualified reference therefore may resolve
              // to the eliminated right scan; refuse the rewrite instead of
              // silently binding it to the left scan.
              if (column.schema.empty() && !column.name.empty() &&
                  column.name != "*") {
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (unique_semi_to_inner_sound)
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
    // Selection(R.key IS NULL [AND left_residual]) into AntiJoin(L, R, p)
    // (wrapped in Selection(left_residual) when left-only conjuncts remain).
    // Sound when R.key is declared NOT NULL OR when ON predicate p rejects
    // NULLs on R.key.
    // Lean proof: lean/TinylambOpt/Discovery.lean
    //   (outer_to_anti_join_aggressive, outer_to_anti_join_equi_nullable_sound,
    //    bug_when_testing_non_join_nullable_col,
    //    bug_when_join_allows_null_match)
    built.Add(Rule(
        "outer_to_anti_join", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group ||
              (memo.Get(input_id).tag.starts_with("outer_to_anti_base:") ||
               memo.Get(input_id).tag.starts_with(
                   "outer_to_anti_base_thru:"))) {
            return;
          }
          if (std::ranges::any_of(memo.Get(input_id).expressions,
                                  [](const LogicalExpression& cand) {
                                    return cand.operation ==
                                           LogicalOperator::kAntiJoin;
                                  })) {
            return;
          }
          const std::vector<LogicalExpression> join_candidates =
              memo.Get(input_id).expressions;
          const std::vector<std::string> input_relations =
              memo.Get(input_id).relations;
          const Expression sel_predicate = *expression.predicate;
          const std::vector<NamedExpression> sel_targets =
              expression.target_list;
          const Schema sel_schema = expression.output_schema;

          for (const LogicalExpression& join_expr : join_candidates) {
            if (join_expr.operation != LogicalOperator::kOuterJoin ||
                join_expr.children.size() != 2 ||
                join_expr.join_type != 0) {  // 0 = LeftOuter
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            const auto& left_rels_vec = memo.Get(left_id).relations;
            const auto& right_rels_vec = memo.Get(right_id).relations;
            const std::unordered_set<std::string> left_rels(
                left_rels_vec.begin(), left_rels_vec.end());
            const std::unordered_set<std::string> right_rels(
                right_rels_vec.begin(), right_rels_vec.end());

            size_t null_checks = 0;
            bool convertible = true;
            std::vector<Expression> left_residuals;
            for (const Expression& conjunct : SplitConjuncts(sel_predicate)) {
              if (!conjunct) {
                convertible = false;
                break;
              }
              if (conjunct->Type() == TypeTag::kUnaryExp &&
                  conjunct->AsUnaryExpression().Op() ==
                      UnaryOperation::kIsNull) {
                const Expression& child = conjunct->AsUnaryExpression().Child();
                if (child && child->Type() == TypeTag::kColumnValue) {
                  const ColumnName& col =
                      child->AsColumnValue().GetColumnName();
                  if (!col.schema.empty() && right_rels.contains(col.schema)) {
                    const bool not_null_in_match =
                        ColumnIsDeclaredNonNull(memo, col.schema, col) ||
                        (join_expr.predicate && *join_expr.predicate &&
                         ExpressionRejectsNullsWhere(
                             *join_expr.predicate,
                             [&](const ColumnName& c) {
                               return c.schema == col.schema &&
                                      c.name == col.name;
                             },
                             /*bare_column_rejects_null=*/false));
                    if (not_null_in_match) {
                      ++null_checks;
                      continue;
                    }
                  }
                }
              }
              // Non-null-test conjunct: allowed as a preserved left-side
              // residual when pure, query/aggregate/window-free, and strictly
              // referencing left_rels.
              if (ContainsAggregate(conjunct) ||
                  TreeContainsWindowCall(conjunct) ||
                  !SafeToReduceEvaluationCount(conjunct)) {
                convertible = false;
                break;
              }
              const auto touched = conjunct->TouchedColumns();
              if (touched.empty() ||
                  !std::ranges::all_of(touched, [&](const ColumnName& c) {
                    return !c.schema.empty() && left_rels.contains(c.schema);
                  })) {
                convertible = false;
                break;
              }
              left_residuals.push_back(conjunct);
            }
            if (!convertible || null_checks == 0) {
              continue;
            }
            const std::vector<NamedExpression>& anti_targets =
                sel_targets.empty() ? join_expr.target_list : sel_targets;
            // AntiJoin emits only preserved-side (left) columns.  Refuse the
            // conversion when the published target list names right-side
            // columns (e.g. `SELECT b.v ... WHERE b.k IS NULL`): the same
            // right-column gate self_join_elimination applies before
            // replacing a join with a single-side scan.
            {
              bool targets_touch_right = false;
              for (const NamedExpression& target : anti_targets) {
                if (!target.expression) {
                  continue;
                }
                for (const ColumnName& column :
                     target.expression->TouchedColumns()) {
                  if (!column.schema.empty() &&
                      right_rels.contains(column.schema)) {
                    targets_touch_right = true;
                  }
                }
              }
              if (targets_touch_right) {
                continue;
              }
            }
            if (left_residuals.empty()) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAntiJoin,
                                    .children = {left_id, right_id},
                                    .predicate = join_expr.predicate,
                                    .target_list = anti_targets,
                                    .output_schema = sel_schema});
            } else {
              const GroupId anti_group = memo.EnsureDerivedGroup(
                  input_relations,
                  "outer_to_anti_base:" + GroupIdsTag({left_id, right_id}) +
                      PredicateTag(join_expr.predicate));
              InheritBranchLocal(memo, anti_group, group);
              if (anti_group == group || anti_group == input_id) {
                continue;
              }
              memo.AddExpression(
                  anti_group,
                  LogicalExpression{.operation = LogicalOperator::kAntiJoin,
                                    .children = {left_id, right_id},
                                    .predicate = join_expr.predicate,
                                    .target_list = join_expr.target_list});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {anti_group},
                                    .predicate = CanonicalizeConjuncts(
                                        CombineConjuncts(left_residuals)),
                                    .target_list = sel_targets,
                                    .output_schema = sel_schema});
            }
          }
        },
        LogicalOperator::kSelection));

    // right_to_left_outer_join: Normalize RightOuterJoin(L, R, p) ->
    // LeftOuterJoin(R, L, p).
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (right_to_left_outer_join_sound)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (full_outer_join_decomposition_sound)
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
            std::string child_key;
            AppendInteger(child_key, left_id);
            child_key.push_back(':');
            AppendInteger(child_key, right_id);
            // The decomposition's target list decides the produced schema;
            // without it two FULL OUTER JOINs with identical children/ON but
            // different projections would share one derived group and leak
            // each other's schema.
            const std::string schema_key =
                ":" + predicate_key + ":" +
                TargetListFingerprint(expression.target_list);
            const std::vector<std::string> cur_relations =
                memo.Get(group).relations;
            const GroupId left_outer_group = memo.EnsureDerivedGroup(
                cur_relations, "full_to_left_outer:" + child_key + schema_key);
            const GroupId anti_join_group = memo.EnsureDerivedGroup(
                cur_relations, "full_to_anti_join:" + child_key + schema_key);
            const GroupId padded_group = memo.EnsureDerivedGroup(
                cur_relations, "full_to_padded_anti:" + child_key + schema_key);
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
    // Lean proof: lean/TinylambOpt/BagRel.lean (outer_to_inner_join_sound,
    //   coalesce_filter_breaks_outer_to_inner) &
    //   lean/TinylambOpt/CppRuleAudit.lean (repairedIsStrict_sound,
    //   repairedRejectsAndNeverFalse_sound, cpp_empty_in_isFalse_bug)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (outer_join_assoc_when_p2_rejects_null_sound,
    //    outer_join_assoc_fails_without_null_reject_bug)
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
            InheritBranchLocal(memo, bc_group, group);
            if (bc_group == group || bc_group == b_id || bc_group == c_id) {
              continue;
            }
            memo.AddExpression(
                bc_group,
                LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {b_id, c_id},
                                  .predicate = expression.predicate,
                                  .join_type = 0});
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (semi_join_reorder_and_pushdown_sound)
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
                semi_expr.children.size() != 2 ||
                (semi_expr.predicate && *semi_expr.predicate &&
                 !SafeToReduceEvaluationCount(*semi_expr.predicate))) {
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
            InheritBranchLocal(memo, rt_group, group);
            if (rt_group == group || rt_group == r_id || rt_group == t_id) {
              continue;
            }
            memo.AddExpression(
                rt_group, LogicalExpression{.operation = LogicalOperator::kJoin,
                                            .children = {r_id, t_id},
                                            .predicate = expression.predicate});
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (semi_join_reorder_and_pushdown_sound)
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
            InheritBranchLocal(memo, rt_group, group);
            if (rt_group == group || rt_group == r_id || rt_group == t_id) {
              continue;
            }
            memo.AddExpression(
                rt_group,
                LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                  .children = {r_id, t_id},
                                  .predicate = expression.predicate});
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
    // in docs/cascades_optimizer.md). Even a uniqueness proof on the right
    // join keys only bounds the match count at one; it does not prove every
    // left row finds a match, so LIMIT pushed into the left side truncates the
    // join's own output whenever an unmatched left row sits inside the limit
    // prefix. That uniqueness proof is now a gate of the separate, ENABLED
    // topn_push_through_proven_one_to_one_join rule below, not of this rule.

    // push_selection_through_window: Push Selection conjuncts that reference
    // only PARTITION BY columns below the Window operator.
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (repaired_push_selection_through_window_sound,
    //    cpp_window_pushdown_mixed_partition_bug)
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
            partition_cols.reserve(win_expr.partition_by.size());
            for (const auto& p : win_expr.partition_by) {
              if (p && p->Type() == TypeTag::kColumnValue) {
                partition_cols.push_back(p->AsColumnValue().GetColumnName());
              }
            }
            // Every WindowFunctionCallExpression in `target_list` must also
            // partition by any column we push on; node-level `partition_by`
            // alone is insufficient on mixed-spec kWindow nodes before
            // `split_window` runs.
            bool all_calls_valid = true;
            for (const NamedExpression& target : win_expr.target_list) {
              if (!target.expression) {
                all_calls_valid = false;
                break;
              }
              if (target.expression->Type() != TypeTag::kWindowFunctionExp) {
                continue;
              }
              const auto& call =
                  target.expression->AsWindowFunctionCallExpression();
              std::vector<ColumnName> call_cols;
              call_cols.reserve(call.partition_by.size());
              for (const Expression& kp : call.partition_by) {
                if (kp && kp->Type() == TypeTag::kColumnValue) {
                  call_cols.push_back(kp->AsColumnValue().GetColumnName());
                }
              }
              std::erase_if(partition_cols, [&](const ColumnName& cand) {
                return !std::ranges::any_of(
                    call_cols, [&](const ColumnName& cc) {
                      return SameColumnReference(cand, cc);
                    });
              });
            }
            if (!all_calls_valid || partition_cols.empty()) {
              continue;
            }

            std::unordered_set<std::string> window_outputs;
            for (const NamedExpression& target : win_expr.target_list) {
              window_outputs.insert(target.name);
            }

            const auto push_conjuncts = SplitConjuncts(*expression.predicate);
            std::vector<Expression> pushable;
            std::vector<Expression> remaining;
            pushable.reserve(push_conjuncts.size());
            remaining.reserve(push_conjuncts.size());
            for (const auto& conjunct : push_conjuncts) {
              if (!conjunct) {
                continue;
              }
              bool can_push = SafeToReduceEvaluationCount(conjunct);
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
            const Expression combined = CombineConjuncts(pushable);
            const GroupId win_child_filtered = memo.EnsureDerivedGroup(
                memo.Get(win_child).relations,
                "win_part_push:" + GroupIdsTag({win_child}) +
                    combined->ToString());
            InheritBranchLocal(memo, win_child_filtered, group);
            if (win_child_filtered == win_child ||
                win_child_filtered == group) {
              continue;
            }
            memo.AddExpression(
                win_child_filtered,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {win_child},
                                  .predicate = combined});

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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (eq_transitivity_and_redundancy_sound)
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
          size_t member_count = 0;
          for (const auto& ec : eq_classes) {
            member_count += ec.size();
          }
          std::unordered_map<std::string, size_t> member_node;
          std::vector<size_t> class_parent;
          member_node.reserve(member_count);
          class_parent.reserve(member_count);
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
              std::string text = inferred_bin->ToString();
              if (known_texts.insert(text).second &&
                  inferred_texts.insert(std::move(text)).second) {
                new_inferred.push_back(std::move(inferred_bin));
              }
            }
          }

          if (!new_inferred.empty()) {
            std::vector<Expression> combined = existing_conjuncts;
            combined.reserve(existing_conjuncts.size() + new_inferred.size());
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (semi_join_reorder_and_pushdown_sound)
    built.Add(Rule(
        "push_semi_join_through_inner_join",
        SemiJoin(Any("inner_join"), Any("c")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSemiJoin ||
              expression.children.size() != 2 || !expression.predicate ||
              !*expression.predicate ||
              !SafeToReduceEvaluationCount(*expression.predicate)) {
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
              InheritBranchLocal(memo, ac_group, group);
              if (ac_group == group || ac_group == a_id || ac_group == c_id) {
                continue;
              }
              memo.AddExpression(
                  ac_group,
                  LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                    .children = {a_id, c_id},
                                    .predicate = expression.predicate});
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
              InheritBranchLocal(memo, bc_group, group);
              if (bc_group == group || bc_group == b_id || bc_group == c_id) {
                continue;
              }
              memo.AddExpression(
                  bc_group,
                  LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                    .children = {b_id, c_id},
                                    .predicate = expression.predicate});
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (push_limit_through_left_join_sound)
    built.Add(Rule(
        "push_limit_through_left_join", Limit(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kLimit ||
              expression.children.size() != 1) {
            return;
          }
          // D5: this equivalence is only sound for an unordered LIMIT -- any
          // subset of the join output is acceptable then. An ordered LIMIT
          // (or WITH TIES, which needs the order to define peers) must keep
          // its position above the join so the restored order picks the same
          // rows; the rewrite below drops every ordering field.
          if (expression.with_ties || !expression.sort_ascending.empty() ||
              !expression.sort_nulls_first.empty() ||
              !expression.target_list.empty()) {
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
            const std::vector<Expression> conjuncts =
                SplitConjuncts(*ojoin.predicate);
            right_join_cols.reserve(conjuncts.size() * 2);
            for (const auto& conjunct : conjuncts) {
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
                  InsertColumnMatcherNames(right_join_cols, col);
                }
              }
              if (bin.Right()->Type() == TypeTag::kColumnValue) {
                const auto& col = bin.Right()->AsColumnValue().GetColumnName();
                if (std::ranges::find(right_rels, col.schema) !=
                    right_rels.end()) {
                  InsertColumnMatcherNames(right_join_cols, col);
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
            for (const Expression& conjunct : conjuncts) {
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

            std::string limit_tag = "ojoin_limit_left:";
            AppendInteger(limit_tag, total_limit);
            limit_tag.push_back(':');
            AppendInteger(limit_tag, left_id);
            limit_tag.push_back(':');
            AppendInteger(limit_tag, right_id);
            limit_tag.push_back(':');
            limit_tag += (*ojoin.predicate)->ToString();
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

            std::string ojoin_tag = "ojoin_limited_left:";
            AppendInteger(ojoin_tag, total_limit);
            ojoin_tag += ':';
            AppendInteger(ojoin_tag, left_id);
            ojoin_tag += ':';
            AppendInteger(ojoin_tag, right_id);
            ojoin_tag += ':';
            ojoin_tag += (*ojoin.predicate)->ToString();
            const GroupId new_ojoin_group = memo.EnsureDerivedGroup(
                memo.Get(input_id).relations, ojoin_tag);
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

    // window_halo_topn: TopN over unpartitioned bounded-ROWS Window sharing the
    // exact ORDER BY spec can restrict the Window input to the sorted halo
    // `[offset - max_preceding, offset + limit + max_following)` via an inner
    // TopN and slice off only `[offset - halo_lo, ...)` above the Window.
    // Lean proof: lean/TinylambOpt/WindowHalo.lean (window_halo_sound)
    built.Add(Rule(
        "window_halo_topn", TopN(Window(Any(), "window")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const char* disable_halo =
              std::getenv("TINYLAMB_DISABLE_WINDOW_HALO");
          if (disable_halo != nullptr && disable_halo[0] != '\0' &&
              disable_halo[0] != '0') {
            return;
          }
          if (expression.operation != LogicalOperator::kTopN ||
              expression.children.size() != 1 || expression.with_ties ||
              expression.limit_count == 0 ||
              expression.prior_limit_offset != 0 ||
              expression.target_list.empty() ||
              expression.target_list.size() !=
                  expression.sort_ascending.size()) {
            return;
          }
          size_t end_excl = 0;
          if (__builtin_add_overflow(expression.limit_offset,
                                     expression.limit_count, &end_excl)) {
            return;
          }
          const GroupId window_group_id = bindings.at("window");
          if (window_group_id == group ||
              memo.Get(window_group_id)
                  .tag.starts_with("window-halo-window:") ||
              memo.Get(window_group_id)
                  .tag.starts_with("window-prefix-window:")) {
            return;
          }
          const std::vector<LogicalExpression> window_exprs =
              memo.Get(window_group_id).expressions;
          const std::vector<NamedExpression> topn_targets =
              expression.target_list;
          const std::vector<bool> topn_ascending = expression.sort_ascending;
          const std::vector<std::optional<bool>> topn_nulls_first =
              expression.sort_nulls_first;
          const size_t topn_limit = expression.limit_count;
          const size_t topn_offset = expression.limit_offset;
          const Schema topn_schema = expression.output_schema;
          for (const LogicalExpression& win : window_exprs) {
            if (win.operation != LogicalOperator::kWindow ||
                win.children.size() != 1 || !win.partition_by.empty() ||
                win.target_list.empty()) {
              continue;
            }
            const GroupId input_id = win.children[0];
            if (input_id == group || input_id == window_group_id ||
                memo.Get(input_id).tag.starts_with("window-halo-topn:") ||
                ReachableChildHasWindow(memo, input_id)) {
              continue;
            }
            const std::vector<std::string> relations =
                memo.Get(input_id).relations;
            auto columns_match = [&](const ColumnName& a, const ColumnName& b) {
              if (SameColumnReference(a, b)) {
                return true;
              }
              if (relations.size() == 1 &&
                  (a.schema.empty() || b.schema.empty())) {
                const std::string& rel = relations.front();
                const bool a_ok =
                    a.schema.empty() || IdentifierEquals(a.schema, rel);
                const bool b_ok =
                    b.schema.empty() || IdentifierEquals(b.schema, rel);
                return a_ok && b_ok && IdentifierEquals(a.name, b.name);
              }
              return false;
            };
            auto is_supported_halo_agg = [](std::string_view fn) {
              return IdentifierEquals(fn, "SUM") ||
                     IdentifierEquals(fn, "COUNT") ||
                     IdentifierEquals(fn, "AVG") ||
                     IdentifierEquals(fn, "MIN") || IdentifierEquals(fn, "MAX");
            };
            size_t max_preceding = 0;
            size_t max_following = 0;
            auto parse_bound = [&](const WindowFrameBound& bound) -> bool {
              if (bound.type == WindowFrameBoundType::kCurrentRow) {
                return true;
              }
              if (bound.type != WindowFrameBoundType::kOffsetPreceding &&
                  bound.type != WindowFrameBoundType::kOffsetFollowing) {
                return false;
              }
              if (!bound.offset ||
                  bound.offset->Type() != TypeTag::kConstantValue ||
                  dynamic_cast<const ParameterSlot*>(bound.offset.get()) !=
                      nullptr) {
                return false;
              }
              const Value& val = bound.offset->AsConstantValue().GetValue();
              if (val.IsNull() || val.type != ValueType::kInt64 ||
                  val.IsUnsigned() || val.value.int_value < 0) {
                return false;
              }
              const auto off = static_cast<size_t>(val.value.int_value);
              if (bound.type == WindowFrameBoundType::kOffsetPreceding) {
                max_preceding = std::max(max_preceding, off);
              } else {
                max_following = std::max(max_following, off);
              }
              return true;
            };

            bool ok = true;
            for (const NamedExpression& target : win.target_list) {
              if (!target.expression ||
                  target.expression->Type() != TypeTag::kWindowFunctionExp) {
                ok = false;
                break;
              }
              const auto& call =
                  target.expression->AsWindowFunctionCallExpression();
              if (!call.partition_by.empty() || !call.has_frame ||
                  call.frame_unit != WindowFrameUnit::kRows ||
                  call.exclusion != WindowFrameExclusion::kNone ||
                  call.where_filter || !call.inner_order_by.empty() ||
                  call.inner_limit.has_value() ||
                  !is_supported_halo_agg(call.function)) {
                ok = false;
                break;
              }
              for (const Expression& arg : call.args) {
                if (!arg || !SafeToReduceEvaluationCount(arg) ||
                    !ExpressionCannotThrowForPlanner(arg)) {
                  ok = false;
                  break;
                }
              }
              if (!ok || !parse_bound(call.frame_start) ||
                  !parse_bound(call.frame_end)) {
                ok = false;
                break;
              }
              if (call.order_by.size() != topn_targets.size()) {
                ok = false;
                break;
              }
              for (size_t j = 0; j < call.order_by.size(); ++j) {
                const WindowOrderTerm& wt = call.order_by[j];
                const Expression& topn_key = topn_targets[j].expression;
                if (!wt.expression || !topn_key) {
                  ok = false;
                  break;
                }
                const bool expr_eq =
                    (wt.expression->Type() == TypeTag::kColumnValue &&
                     topn_key->Type() == TypeTag::kColumnValue)
                        ? columns_match(
                              wt.expression->AsColumnValue().GetColumnName(),
                              topn_key->AsColumnValue().GetColumnName())
                        : (wt.expression->ToString() == topn_key->ToString());
                const std::optional<bool> topn_nulls =
                    j < topn_nulls_first.size() ? topn_nulls_first[j]
                                                : std::nullopt;
                if (!expr_eq || wt.ascending != topn_ascending[j] ||
                    wt.nulls_first.value_or(wt.ascending) !=
                        topn_nulls.value_or(wt.ascending) ||
                    CollationOfExpression(wt.expression) !=
                        CollationOfExpression(topn_key)) {
                  ok = false;
                  break;
                }
              }
              if (!ok) {
                break;
              }
            }
            if (!ok) {
              continue;
            }
            const size_t halo_lo =
                topn_offset > max_preceding ? topn_offset - max_preceding : 0;
            size_t halo_hi = 0;
            if (__builtin_add_overflow(end_excl, max_following, &halo_hi)) {
              continue;
            }
            const size_t halo_count = halo_hi - halo_lo;
            if (halo_count == 0) {
              continue;
            }
            const size_t outer_offset = topn_offset - halo_lo;

            std::string sig = GroupIdsTag({input_id}) + "|" +
                              WindowNodeIdentity(win) + "|" + win.Fingerprint();
            AppendInteger(sig, halo_lo);
            sig.push_back(':');
            AppendInteger(sig, halo_count);
            sig.push_back('|');
            for (size_t j = 0; j < topn_targets.size(); ++j) {
              const std::string text = topn_targets[j].expression->ToString();
              AppendInteger(sig, text.size());
              sig.push_back(':');
              sig.append(text);
              sig.push_back(topn_ascending[j] ? '+' : '-');
              const std::optional<bool> nf = j < topn_nulls_first.size()
                                                 ? topn_nulls_first[j]
                                                 : std::nullopt;
              sig.push_back(nf ? (*nf ? '1' : '0') : '?');
            }

            const GroupId halo_topn_group =
                memo.EnsureDerivedGroup(relations, "window-halo-topn:" + sig);
            if (halo_topn_group == group ||
                halo_topn_group == window_group_id ||
                halo_topn_group == input_id) {
              continue;
            }
            memo.AddExpression(
                halo_topn_group,
                LogicalExpression{.operation = LogicalOperator::kTopN,
                                  .children = {input_id},
                                  .target_list = topn_targets,
                                  .sort_ascending = topn_ascending,
                                  .sort_nulls_first = topn_nulls_first,
                                  .limit_count = halo_count,
                                  .limit_offset = halo_lo,
                                  .with_ties = false});

            const GroupId halo_win_group =
                memo.EnsureDerivedGroup(relations, "window-halo-window:" + sig);
            if (halo_win_group == group || halo_win_group == window_group_id ||
                halo_win_group == input_id ||
                halo_win_group == halo_topn_group) {
              continue;
            }
            LogicalExpression new_win = win;
            new_win.children = {halo_topn_group};
            memo.AddExpression(halo_win_group, std::move(new_win));

            memo.AddExpression(
                group, LogicalExpression{.operation = LogicalOperator::kTopN,
                                         .children = {halo_win_group},
                                         .target_list = topn_targets,
                                         .sort_ascending = topn_ascending,
                                         .sort_nulls_first = topn_nulls_first,
                                         .limit_count = topn_limit,
                                         .limit_offset = outer_offset,
                                         .prior_limit_offset = halo_lo,
                                         .with_ties = false,
                                         .output_schema = topn_schema});
          }
        },
        LogicalOperator::kTopN));

    // push_topn_with_ties_through_left_join: TopN ordered strictly on left
    // columns over LeftOuterJoin can push `TopN(offset + limit WITH TIES)`
    // into the preserved left branch because every left row emits >= 1 output
    // row in LEFT JOIN and ties keep all peers of the boundary key.
    // Lean proof: lean/TinylambOpt/OuterFetchTies.lean
    //   (sql_outer_fetch_ties_sound)
    built.Add(Rule(
        "push_topn_with_ties_through_left_join", TopN(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const char* disable_ties =
              std::getenv("TINYLAMB_DISABLE_OUTER_FETCH_TIES");
          if (disable_ties != nullptr && disable_ties[0] != '\0' &&
              disable_ties[0] != '0') {
            return;
          }
          if (expression.operation != LogicalOperator::kTopN ||
              expression.children.size() != 1 || expression.limit_count == 0 ||
              expression.prior_limit_offset != 0 ||
              expression.target_list.empty() ||
              expression.target_list.size() !=
                  expression.sort_ascending.size()) {
            return;
          }
          size_t total_limit = 0;
          if (__builtin_add_overflow(expression.limit_count,
                                     expression.limit_offset, &total_limit) ||
              total_limit == 0) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group ||
              memo.Get(input_id).tag.starts_with("ojoin_topn_ties_join:")) {
            return;
          }
          auto is_pure_join_side_expr = [](auto&& self,
                                           const Expression& expr) -> bool {
            if (!expr) {
              return false;
            }
            switch (expr->Type()) {
              case TypeTag::kColumnValue:
                return true;
              case TypeTag::kConstantValue:
                return dynamic_cast<const ParameterSlot*>(expr.get()) ==
                       nullptr;
              case TypeTag::kBinaryExp: {
                const auto& b = expr->AsBinaryExpression();
                if (b.Op() == BinaryOperation::kDivide ||
                    b.Op() == BinaryOperation::kModulo ||
                    b.Op() == BinaryOperation::kShiftLeft ||
                    b.Op() == BinaryOperation::kShiftRight) {
                  return false;
                }
                return self(self, b.Left()) && self(self, b.Right());
              }
              case TypeTag::kUnaryExp: {
                const auto& u = expr->AsUnaryExpression();
                return u.Op() != UnaryOperation::kMinus &&
                       self(self, u.Child());
              }
              case TypeTag::kInExp:
              case TypeTag::kCaseExp: {
                const auto children = ExpressionChildren(expr);
                return !children.empty() &&
                       std::ranges::all_of(children, [&](const Expression& ch) {
                         return self(self, ch);
                       });
              }
              case TypeTag::kFunctionCallExp: {
                const auto& fn = expr->AsFunctionCallExpression();
                if (IdentifierEquals(fn.FuncName(), "rand") ||
                    IdentifierEquals(fn.FuncName(), "random") ||
                    IdentifierEquals(fn.FuncName(), "now") ||
                    IdentifierEquals(fn.FuncName(), "current_timestamp") ||
                    IdentifierEquals(fn.FuncName(), "uuid") ||
                    IdentifierEquals(fn.FuncName(), "generate_uuid")) {
                  return false;
                }
                return std::ranges::all_of(
                    fn.Args(),
                    [&](const Expression& ch) { return self(self, ch); });
              }
              default:
                return false;
            }
          };

          const LogicalExpression& outer_template = expression;
          const std::vector<LogicalExpression> input_exprs =
              memo.Get(input_id).expressions;
          const std::vector<std::string> join_rels =
              memo.Get(input_id).relations;
          for (const LogicalExpression& ojoin : input_exprs) {
            if (ojoin.operation != LogicalOperator::kOuterJoin ||
                ojoin.join_type != 0 || ojoin.children.size() != 2 ||
                !ojoin.predicate.has_value() || !*ojoin.predicate ||
                !is_pure_join_side_expr(is_pure_join_side_expr,
                                        *ojoin.predicate)) {
              continue;
            }
            const GroupId left_id = ojoin.children[0];
            const GroupId right_id = ojoin.children[1];
            if (memo.Get(left_id).tag.starts_with("ojoin_topn_ties_left:")) {
              continue;
            }
            bool left_already_bounded = false;
            for (const LogicalExpression& lexpr :
                 memo.Get(left_id).expressions) {
              if ((lexpr.operation == LogicalOperator::kTopN &&
                   lexpr.with_ties && lexpr.limit_offset == 0 &&
                   lexpr.limit_count <= total_limit) ||
                  (lexpr.operation == LogicalOperator::kLimit &&
                   lexpr.limit_offset == 0 &&
                   lexpr.limit_count <= total_limit)) {
                left_already_bounded = true;
                break;
              }
            }
            if (left_already_bounded) {
              continue;
            }

            const std::vector<std::string> left_rels =
                memo.Get(left_id).relations;
            const std::vector<std::string> right_rels =
                memo.Get(right_id).relations;
            auto rel_contains = [](const std::vector<std::string>& rels,
                                   const std::string& schema) {
              return std::ranges::any_of(rels, [&](const std::string& rel) {
                return IdentifierEquals(rel, schema);
              });
            };
            bool all_left_only = true;
            bool touched_any = false;
            for (const NamedExpression& key : outer_template.target_list) {
              if (!key.expression ||
                  !is_pure_join_side_expr(is_pure_join_side_expr,
                                          key.expression)) {
                all_left_only = false;
                break;
              }
              const auto touched = key.expression->TouchedColumns();
              for (const ColumnName& col : touched) {
                touched_any = true;
                if (col.schema.empty() ||
                    !rel_contains(left_rels, col.schema) ||
                    rel_contains(right_rels, col.schema)) {
                  all_left_only = false;
                  break;
                }
              }
              if (!all_left_only) {
                break;
              }
            }
            if (!all_left_only || !touched_any) {
              continue;
            }

            std::string sig;
            AppendInteger(sig, total_limit);
            sig.push_back(':');
            AppendInteger(sig, left_id);
            sig.push_back(':');
            AppendInteger(sig, right_id);
            sig.push_back(':');
            const std::string pred_str = (*ojoin.predicate)->ToString();
            AppendInteger(sig, pred_str.size());
            sig.push_back('/');
            sig.append(pred_str);
            sig.push_back('|');
            for (size_t j = 0; j < outer_template.target_list.size(); ++j) {
              const std::string key_str =
                  outer_template.target_list[j].expression->ToString();
              AppendInteger(sig, key_str.size());
              sig.push_back(':');
              sig.append(key_str);
              sig.push_back(outer_template.sort_ascending[j] ? '+' : '-');
              const std::optional<bool> nf =
                  j < outer_template.sort_nulls_first.size()
                      ? outer_template.sort_nulls_first[j]
                      : std::nullopt;
              sig.push_back(nf ? (*nf ? '1' : '0') : '?');
            }

            const GroupId topn_left_group = memo.EnsureDerivedGroup(
                left_rels, "ojoin_topn_ties_left:" + sig);
            if (topn_left_group == left_id || topn_left_group == group ||
                topn_left_group == input_id) {
              continue;
            }
            memo.AddExpression(
                topn_left_group,
                LogicalExpression{
                    .operation = LogicalOperator::kTopN,
                    .children = {left_id},
                    .target_list = outer_template.target_list,
                    .sort_ascending = outer_template.sort_ascending,
                    .sort_nulls_first = outer_template.sort_nulls_first,
                    .limit_count = total_limit,
                    .limit_offset = 0,
                    .with_ties = true});

            const GroupId new_ojoin_group = memo.EnsureDerivedGroup(
                join_rels, "ojoin_topn_ties_join:" + sig);
            if (new_ojoin_group == group || new_ojoin_group == input_id ||
                new_ojoin_group == topn_left_group) {
              continue;
            }
            LogicalExpression new_ojoin = ojoin;
            new_ojoin.children = {topn_left_group, right_id};
            memo.AddExpression(new_ojoin_group, std::move(new_ojoin));

            LogicalExpression outer_topn = outer_template;
            outer_topn.children = {new_ojoin_group};
            memo.AddExpression(group, std::move(outer_topn));
          }
        },
        LogicalOperator::kTopN));

    // merge_adjacent_windows: Fuse Window_1(Window_2(X)) into a single
    // Window(X) when both windows have identical PARTITION BY and Window_1
    // does not reference Window_2's outputs. ORDER BY/frame specifications
    // remain on each target's WindowFunctionCallExpression.
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (window_stack_and_merge_sound)
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
            merged_targets.reserve(inner_win.target_list.size() +
                                   expression.target_list.size());
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
    // Lean proof: lean/TinylambOpt/ThreeValued.lean (double_negation_sound,
    //   de_morgan_and_sound, de_morgan_or_sound, not_comparison_unsound_on_nan)
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (no_op_window_elimination_sound,
    //    cpp_no_op_window_star_and_qualified_bug)
    built.Add(Rule(
        "no_op_window_elimination", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation == LogicalOperator::kWindow &&
              expression.children.size() == 1) {
            const GroupId child = expression.children[0];
            if (child != group) {
              if (expression.target_list.empty()) {
                // Same filter contract as eliminate_true_selection: the
                // copied alternatives implement against this group's filter,
                // so a scan filter living only on the child group must move
                // with them (kWindow is unary and preserves the relation
                // set, so a non-null child filter implies single-relation).
                const Group& child_group = memo.Get(child);
                if (child_group.filter &&
                    memo.Get(group).relations.size() == 1) {
                  // The child's filter can be branch-local; the copied
                  // alternatives must keep that mark or a later rule will
                  // push it into a scan shared by another set-op branch.
                  InheritBranchLocal(memo, group, child);
                  memo.MergeScanFilter(group, child_group.filter);
                }
                for (const LogicalExpression& child_expr :
                     child_group.expressions) {
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
              // The outer targets must not reference the window outputs
              // (whether bare `$winN`, qualified `sub.w`, or `*` wildcard),
              // nor contain raw window calls.
              const auto outer_uses_window = [](const LogicalExpression& outer,
                                                const LogicalExpression& win) {
                if (outer.target_list.empty()) {
                  // Unbound target list (e.g. SELECT * before expansion):
                  // it may expose the window outputs even though nothing is
                  // named yet, so treat it as a reference conservatively.
                  return true;
                }
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
                    if (column.name == "*" || outputs.contains(column.name)) {
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

    // Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
    // (filter_transfer_preserves_matches,
    // filter_transfer_two_hop_preserves_matches,
    //  transfer_filter_through_dimension_sound)
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (dynamic_filter_and_semi_reduction_sound,
    //    cpp_dynamic_filter_erases_derived_probe_bug)
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
          if (probe_rels.size() != 1 || memo.Get(probe_id).sample_barrier ||
              !std::ranges::any_of(
                  memo.Get(probe_id).expressions,
                  [&](const LogicalExpression& candidate) {
                    return candidate.operation == LogicalOperator::kScan &&
                           candidate.table == probe_rels.front();
                  })) {
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
          // Length-prefix each name: a quoted identifier may itself contain
          // the separator, and two different key sets must never collapse to
          // the same derived-group tag.
          std::string key_fingerprint;
          for (const ColumnName& key : bloom_keys) {
            const std::string name = key.ToString();
            key_fingerprint += std::to_string(name.size());
            key_fingerprint.push_back(':');
            key_fingerprint += name;
          }
          const GroupId filtered_probe = memo.EnsureDerivedGroup(
              probe_rels,
              "bloom_probe:" + GroupIdsTag({probe_id}) + key_fingerprint);
          if (filtered_probe != probe_id && filtered_probe != group) {
            memo.Get(filtered_probe).filter = probe_filter;
            // A branch-local probe must stay branch-local: other derived rules
            // inherit the mark, otherwise this shared group could leak the
            // probe filter into another UNION branch.
            InheritBranchLocal(memo, filtered_probe, group);
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
          if (probe_rels.size() != 1 || memo.Get(probe_id).sample_barrier ||
              !std::ranges::any_of(
                  memo.Get(probe_id).expressions,
                  [&](const LogicalExpression& candidate) {
                    return candidate.operation == LogicalOperator::kScan &&
                           candidate.table == probe_rels.front();
                  })) {
            return;
          }
          const std::vector<Expression> probe_conjuncts =
              SplitConjuncts(*expression.predicate);
          std::vector<ColumnName> probe_keys;
          probe_keys.reserve(probe_conjuncts.size());
          for (const Expression& conjunct : probe_conjuncts) {
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
          // Length-prefix each key: a plain comma-separated concatenation
          // collides for e.g. ["a", "b"] and ["a,b"], which would reuse a
          // derived group annotated with the wrong probe key set.
          std::string key_fingerprint = std::to_string(probe_keys.size()) + ":";
          for (const ColumnName& key : probe_keys) {
            const std::string text = key.ToString();
            key_fingerprint += std::to_string(text.size()) + "/" + text + ";";
          }
          const GroupId keyed_probe = memo.EnsureDerivedGroup(
              probe_rels,
              "semi_reduce_probe:" + GroupIdsTag({probe_id}) + key_fingerprint);
          if (keyed_probe != probe_id && keyed_probe != group) {
            memo.Get(keyed_probe).filter = probe_filter;
            // Keep the probe's branch locality: without this the derived
            // group may be shared by another UNION branch as well.
            InheritBranchLocal(memo, keyed_probe, group);
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (dynamic_filter_and_semi_reduction_sound,
    //    cpp_dynamic_filter_erases_derived_probe_bug)
    built.Add(Rule("semi_reduction_pushdown_semi_join",
                   SemiJoin(Any("probe"), Any("build")),
                   semi_reduction_annotate, LogicalOperator::kSemiJoin));
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (dynamic_filter_and_semi_reduction_sound,
    //    cpp_dynamic_filter_erases_derived_probe_bug)
    built.Add(Rule("semi_reduction_pushdown_anti_join",
                   AntiJoin(Any("probe"), Any("build")),
                   semi_reduction_annotate, LogicalOperator::kAntiJoin));

    // join_predicate_transitivity: Infer non-constant equality transitive
    // chains across multi-way joins (t1.a = t2.b and t2.b = t3.c -> t1.a =
    // t3.c).
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (eq_transitivity_and_redundancy_sound)
    built.Add(Rule(
        "join_predicate_transitivity", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate) {
            return;
          }
          const auto conjuncts = SplitConjuncts(*expression.predicate);
          std::vector<std::pair<ColumnName, ColumnName>> equalities;
          equalities.reserve(conjuncts.size());
          for (const auto& conj : conjuncts) {
            if (conj && conj->Type() == TypeTag::kBinaryExp) {
              const auto& bin = conj->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kEquals &&
                  bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kColumnValue) {
                const ColumnName lhs =
                    bin.Left()->AsColumnValue().GetColumnName();
                const ColumnName rhs =
                    bin.Right()->AsColumnValue().GetColumnName();
                if (!ColumnsProvenDifferentType(memo, lhs, rhs)) {
                  equalities.emplace_back(lhs, rhs);
                }
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (same_domain_infer_join_constants_and_inequalities_sound,
    //    cpp_cross_type_int64_double_nontransitive_bug)
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (eq_transitivity_and_redundancy_sound,
    //    cpp_redundant_join_eq_drops_self_eq_null_bug)
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
          std::unordered_set<std::string> constrained;
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
            // SQL `=` is NOT reflexive on NULL or NaN (`NULL = NULL` is
            // UNKNOWN, `NaN = NaN` is FALSE), whereas union-find `root(u) ==
            // root(u)` is always true. A self-equality `u = u` is redundant
            // ONLY when an earlier kept equality already constrained `u` to be
            // non-NULL and non-NaN.
            if (a == b) {
              if (constrained.contains(a)) {
                return false;
              }
              constrained.insert(a);
              return true;
            }
            std::string root_a = find_root(find_root, a);
            std::string root_b = find_root(find_root, b);
            constrained.insert(a);
            constrained.insert(b);
            if (root_a == root_b) {
              return false;
            }
            parent[root_a] = root_b;
            return true;
          };

          std::vector<Expression> kept;
          kept.reserve(conjuncts.size());
          bool removed = false;
          for (const auto& conj : conjuncts) {
            if (conj && conj->Type() == TypeTag::kBinaryExp) {
              const auto& bin = conj->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kEquals && bin.Left() &&
                  bin.Right() && bin.Left()->Type() == TypeTag::kColumnValue &&
                  bin.Right()->Type() == TypeTag::kColumnValue) {
                const ColumnName lhs_col =
                    bin.Left()->AsColumnValue().GetColumnName();
                const ColumnName rhs_col =
                    bin.Right()->AsColumnValue().GetColumnName();
                if (!ColumnsProvenDifferentType(memo, lhs_col, rhs_col)) {
                  const std::string u = lhs_col.ToString();
                  const std::string v = rhs_col.ToString();
                  if (unite(u, v)) {
                    kept.push_back(conj);
                  } else {
                    removed = true;
                  }
                  continue;
                }
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (unnest_and_values_rules_sound,
    //    cpp_unnest_alias_and_empty_array_throw_bugs)
    built.Add(Rule(
        "unnest_filter_pushdown", Selection(Unnest(Any(), "unnest")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          auto free_of_data_dependent_errors =
              [](const auto& self, const Expression& expr) -> bool {
            if (!expr) {
              return false;
            }
            switch (expr->Type()) {
              case TypeTag::kConstantValue:
              case TypeTag::kColumnValue:
                return true;
              case TypeTag::kUnaryExp: {
                const auto& u = expr->AsUnaryExpression();
                switch (u.Op()) {
                  case UnaryOperation::kIsNull:
                  case UnaryOperation::kIsNotNull:
                  case UnaryOperation::kNot:
                  case UnaryOperation::kIsTrue:
                  case UnaryOperation::kIsNotTrue:
                  case UnaryOperation::kIsFalse:
                  case UnaryOperation::kIsNotFalse:
                    return self(self, u.Child());
                  default:
                    return false;
                }
              }
              case TypeTag::kBinaryExp: {
                const auto& b = expr->AsBinaryExpression();
                switch (b.Op()) {
                  case BinaryOperation::kEquals:
                  case BinaryOperation::kNotEquals:
                  case BinaryOperation::kLessThan:
                  case BinaryOperation::kLessThanEquals:
                  case BinaryOperation::kGreaterThan:
                  case BinaryOperation::kGreaterThanEquals:
                  case BinaryOperation::kIsDistinctFrom:
                  case BinaryOperation::kIsNotDistinctFrom:
                  case BinaryOperation::kAnd:
                  case BinaryOperation::kOr:
                  case BinaryOperation::kXor:
                    return self(self, b.Left()) && self(self, b.Right());
                  default:
                    return false;
                }
              }
              case TypeTag::kInExp: {
                const auto& in = expr->AsInExpression();
                return self(self, in.child_) &&
                       std::ranges::all_of(in.list_, [&](const Expression& e) {
                         return self(self, e);
                       });
              }
              default:
                return false;
            }
          };
          if (!expression.predicate || !*expression.predicate ||
              !free_of_data_dependent_errors(free_of_data_dependent_errors,
                                             *expression.predicate) ||
              !SafeToReduceEvaluationCount(*expression.predicate)) {
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
            const std::string eff_alias =
                unnest.unnest_alias.empty() ? "unnest" : unnest.unnest_alias;
            const std::unordered_set<ColumnName> touched =
                (*expression.predicate)->TouchedColumns();
            bool touches_unnest = false;
            for (const auto& col : touched) {
              if (col.name == "*" || IdentifierEquals(col.name, eff_alias) ||
                  (!col.schema.empty() &&
                   IdentifierEquals(col.schema, eff_alias)) ||
                  (!unnest.offset_alias.empty() &&
                   IdentifierEquals(col.name, unnest.offset_alias))) {
                touches_unnest = true;
                break;
              }
            }
            if (touches_unnest) {
              continue;
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (unnest_and_values_rules_sound, cpp_parameter_slot_freeze_bug)
    built.Add(Rule(
        "constant_unnest_to_values", Unnest(Any("input"), "unnest"),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate ||
              ContainsParameterSlot(*expression.predicate)) {
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
          const auto& elements = evaluated.Value().ArrayElements();
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (recursive_termination_predicate_pushdown_sound,
    //    cpp_parameter_slot_freeze_bug)
    built.Add(Rule(
        "recursive_termination_predicate_pushdown", Selection(Any("cte")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate ||
              ContainsParameterSlot(*expression.predicate)) {
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
    // with a Values group.
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (in_list_to_semi_join_sound, cpp_in_list_fingerprint_collision_bug)
    built.Add(Rule(
        "in_list_to_semi_join", Selection(Any("child")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate ||
              ContainsParameterSlot(*expression.predicate)) {
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
          // type- and length-prefix every constant so `['a;b','c']` never
          // collides with `['a','b;c']` (Lean:
          // `cpp_in_list_fingerprint_collision_bug`).
          std::string value_fingerprint = target_col->ToString() + ":";
          for (const Value& v : in_values) {
            const std::string text = v.AsString();
            AppendInteger(value_fingerprint, static_cast<int>(v.type));
            value_fingerprint.push_back(':');
            AppendInteger(value_fingerprint, text.size());
            value_fingerprint.push_back(':');
            value_fingerprint += text;
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (eq_transitivity_and_redundancy_sound)
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
                const Expression* column_expr = nullptr;
                const Expression* bound_expr = nullptr;
                BinaryOperation op = bin.Op();
                if (bin.Left()->Type() == TypeTag::kColumnValue &&
                    bin.Right()->Type() == TypeTag::kConstantValue) {
                  column_expr = &bin.Left();
                  bound_expr = &bin.Right();
                } else if (bin.Left()->Type() == TypeTag::kConstantValue &&
                           bin.Right()->Type() == TypeTag::kColumnValue) {
                  column_expr = &bin.Right();
                  bound_expr = &bin.Left();
                  switch (op) {
                    case BinaryOperation::kLessThan:
                      op = BinaryOperation::kGreaterThan;
                      break;
                    case BinaryOperation::kGreaterThan:
                      op = BinaryOperation::kLessThan;
                      break;
                    case BinaryOperation::kLessThanEquals:
                      op = BinaryOperation::kGreaterThanEquals;
                      break;
                    case BinaryOperation::kGreaterThanEquals:
                      op = BinaryOperation::kLessThanEquals;
                      break;
                    default:
                      break;
                  }
                }
                if (column_expr != nullptr) {
                  const std::string col_str = (*column_expr)
                                                  ->AsColumnValue()
                                                  .GetColumnName()
                                                  .ToString();
                  auto it = eq_constants.find(col_str);
                  if (it != eq_constants.end()) {
                    const Value& k = it->second;
                    const Value& bound =
                        (*bound_expr)->AsConstantValue().GetValue();
                    if (!k.IsNull() && !bound.IsNull() &&
                        k.type == bound.type) {
                      const StatusOr<Value> eval =
                          TryEvaluateBinary(op, k, bound);
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
    // Lean proof: lean/TinylambOpt/CppRuleAudit.lean
    //   (repaired_filter_aggregate_pushdown_sound,
    //    cpp_filter_agg_pushdown_compound_target_bug)
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
            new_targets.reserve(expression.target_list.size());
            bool transformed = false;
            bool all_valid = true;
            for (const auto& target : expression.target_list) {
              if (!target.expression) {
                all_valid = false;
                break;
              }
              if (target.expression->Type() == TypeTag::kAggregateExp) {
                const auto& orig = dynamic_cast<const AggregateExpression&>(
                    *target
                         .expression);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
                if (orig.NeedsGroupContext() || !orig.InnerGroupBy().empty()) {
                  all_valid = false;
                  break;
                }
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
              } else if (!ContainsAggregate(target.expression) &&
                         target.expression->TouchedColumns().empty()) {
                new_targets.push_back(target);
              } else {
                // Compound expressions containing aggregates (`SUM(x) + 1`) or
                // bare column references would lose the Selection filter if
                // passed through untouched.
                all_valid = false;
                break;
              }
            }
            if (all_valid && transformed) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {input_id},
                                    .predicate = expression.predicate,
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
    // Lean proof: lean/TinylambOpt/CppRuleAudit.lean
    //   (repaired_agg_union_sum_and_count_sound,
    //    cpp_agg_union_compound_sum_plus_one_bug,
    //    cpp_agg_union_having_pushed_to_branch_bug) &
    //   lean/TinylambOpt/AggBase.lean (aggFold_append)
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

            std::vector<ColumnName> group_key_cols;
            bool keys_ok = true;
            for (const Expression& g : expression.grouping_sets) {
              if (!g || g->Type() != TypeTag::kColumnValue) {
                keys_ok = false;
                break;
              }
              group_key_cols.push_back(g->AsColumnValue().GetColumnName());
            }
            if (!keys_ok) {
              continue;
            }

            bool distributable = true;
            bool has_agg = false;
            std::unordered_set<std::string> seen_agg_names;
            for (const auto& target : expression.target_list) {
              if (!target.expression) {
                distributable = false;
                break;
              }
              if (target.expression->Type() == TypeTag::kAggregateExp) {
                const auto& agg = dynamic_cast<const AggregateExpression&>(
                    *target
                         .expression);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
                if (target.name.empty() ||
                    !seen_agg_names.insert(target.name).second ||
                    agg.Distinct() || agg.NeedsGroupContext() ||
                    agg.HasWhereFilter() || !agg.InnerGroupBy().empty()) {
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
                has_agg = true;
              } else {
                // Compound aggregate targets (`SUM(x) + 1`, `AVG(x) + 1`) are
                // NOT homomorphisms over UNION ALL (`(s1 + 1) + (s2 + 1) !=
                // (s1 + s2) + 1`). Non-aggregate targets may only reference
                // plain grouping columns preserved by the per-branch schema.
                if (ContainsAggregate(target.expression)) {
                  distributable = false;
                  break;
                }
                for (const ColumnName& touched :
                     target.expression->TouchedColumns()) {
                  const bool in_group_keys = std::ranges::any_of(
                      group_key_cols, [&](const ColumnName& gk) {
                        return gk == touched ||
                               ((gk.schema.empty() || touched.schema.empty()) &&
                                gk.name == touched.name);
                      });
                  if (!in_group_keys) {
                    distributable = false;
                    break;
                  }
                }
                if (!distributable) {
                  break;
                }
              }
            }
            if (!distributable || !has_agg) {
              continue;
            }

            std::vector<GroupId> pushed_aggs;
            pushed_aggs.reserve(union_expr.children.size());
            const std::string target_fingerprint =
                TargetListFingerprint(expression.target_list);
            for (size_t i = 0; i < union_expr.children.size(); ++i) {
              const GroupId branch_id = union_expr.children[i];
              // Keyed by the branch group itself (D1): two unions over the
              // same relation set must never share a per-branch aggregate.
              // Per-branch aggregates NEVER inherit HAVING (`predicate`);
              // HAVING applies only after cross-branch re-aggregation.
              const GroupId branch_agg = memo.EnsureDerivedGroup(
                  memo.Get(branch_id).relations,
                  "agg_union_branch_" + std::to_string(i) + ":" +
                      std::to_string(branch_id) + ":" + target_fingerprint);
              InheritBranchLocal(memo, branch_agg, group);
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
              AppendInteger(union_tag, pushed);
              union_tag += ',';
            }
            const GroupId union_of_aggs =
                memo.EnsureDerivedGroup(memo.Get(group).relations, union_tag);
            InheritBranchLocal(memo, union_of_aggs, group);
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
                  const auto& agg = dynamic_cast<
                      const AggregateExpression&>(  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
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
                                    .predicate = expression.predicate,
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (pk_unique_distinct_elim_sound)
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
              columns.reserve(child.target_list.size());
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
              columns.reserve(schema->ColumnCount());
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (not_null_and_trivial_selection_sound)
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
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (core_join_identity_and_empty_sound)
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
          const Group& survivor_group = memo.Get(survivor);
          if (survivor_group.relations != memo.Get(group).relations) {
            return;
          }
          // The copied alternatives implement against this group's filter,
          // so a scan filter living only on the survivor must move with them
          // (same contract as eliminate_true_selection). A non-null filter
          // implies a single-relation survivor and this unary reduction
          // preserves the relation set, so the destination is single too.
          if (survivor != group && survivor_group.filter &&
              memo.Get(group).relations.size() == 1) {
            // Same contract as eliminate_true_selection: carry the survivor's
            // branch-local mark with the filter so it is never later merged
            // into a scan shared by another set-op branch.
            InheritBranchLocal(memo, group, survivor);
            memo.MergeScanFilter(group, survivor_group.filter);
          }
          for (const LogicalExpression& alternative :
               survivor_group.expressions) {
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
    // Lean proof: lean/TinylambOpt/CppRuleAudit.lean
    //   (count_star_rewrite_on_not_null_sound)
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
    // the right join keys only bounds the match count at one. The unmatched
    // left row inside the top-(limit+offset) prefix is that DISABLED rule's
    // counterexample, not a gate of the enabled rule below. Proving
    // ">= 1 match" needs referential-integrity metadata the memo does not
    // model.

    // topn_push_through_proven_one_to_one_join:
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (topn_push_through_proven_one_to_one_join_sound,
    //    topn_push_inner_join_without_fk_unsound)
    built.Add(Rule(
        "topn_push_through_proven_one_to_one_join", TopN(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kTopN ||
              expression.children.size() != 1 || expression.with_ties ||
              expression.prior_limit_offset != 0) {
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
              pushed_join_cols.reserve(join_conjuncts.size() * 2);
              other_join_cols.reserve(join_conjuncts.size() * 2);
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
                InsertColumnMatcherNames(pushed_join_cols, *pushed_col);
                InsertColumnMatcherNames(other_join_cols, *other_col);
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
              // The declared-FK fallback below is scan-free: it trusts the
              // catalog, so it cannot look at the rows that actually survive
              // on the other side. Any single-relation predicate of the query
              // already lives in the other-side group's scan filter
              // (push_selection_through_join merges it there), and such a
              // filter can remove exactly the row the FOREIGN KEY
              // certificate guarantees -- `WHERE b.v > 5` on b drops the
              // guaranteed parent, so a fact row inside the pushed
              // top-(limit+offset) prefix would emit a row the plain join
              // filters away. The other side must therefore be
              // filter-free for the FK fallback; the snapshot proof
              // (MarkProvenOneToOneJoins) is the filter-aware alternative,
              // and its own WHERE gate keeps it to predicates that cannot
              // rescue a filtered other side.
              if (memo.Get(other_id).filter) {
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
                AppendInteger(keys_sig, key.name.size());
                keys_sig.push_back(':');
                keys_sig.append(key.name);
                keys_sig.push_back('=');
                keys_sig.append(key.expression ? key.expression->ToString()
                                               : std::string("<null>"));
                keys_sig.push_back('|');
              }
              AppendInteger(keys_sig, topn_ascending.size());
              keys_sig.push_back(':');
              for (const bool ascending : topn_ascending) {
                keys_sig.push_back(ascending ? '1' : '0');
              }
              keys_sig.push_back('/');
              AppendInteger(keys_sig, topn_nulls_first.size());
              keys_sig.push_back(':');
              for (const std::optional<bool>& nulls_first : topn_nulls_first) {
                keys_sig.push_back(nulls_first.has_value()
                                       ? (nulls_first.value() ? '1' : '0')
                                       : '?');
              }
              const std::vector<std::string> pushed_relations =
                  memo.Get(pushed_id).relations;
              std::string tag = "topn_proven_one_to_one:";
              AppendInteger(tag, total_limit);
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
              AppendInteger(pushed_join_tag, total_limit);
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
    // Lean proof: lean/TinylambOpt/ThreeValued.lean (or_branches_disjoint,
    //   or_branches_exact_count, naive_or_split_loses_null_row) &
    //   lean/TinylambOpt/BagRel.lean (or_to_union_preserves_bag)
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
              const auto& schemas = memo.GetTableSchemas();
              for (const ColumnName& column : current->TouchedColumns()) {
                if (!column.schema.empty()) {
                  if (std::ranges::find(input_relations, column.schema) ==
                      input_relations.end()) {
                    return false;
                  }
                  continue;
                }
                // An unqualified column must also resolve to an input
                // relation: an outer reference would be re-bound per UNION
                // branch and could evaluate differently in each. Keep the
                // permissive behaviour when schemas are unavailable (e.g.
                // synthesized memos in unit tests).
                if (schemas.empty()) {
                  continue;
                }
                size_t matches = 0;
                for (const auto& rel : input_relations) {
                  const auto found = schemas.find(rel);
                  if (found != schemas.end() &&
                      found->second.Offset(ColumnName("", column.name)) >= 0) {
                    ++matches;
                  }
                }
                if (matches != 1) {
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
            std::string branch_tag = "or_union_branch:";
            AppendInteger(branch_tag, input_id);
            branch_tag += ':';
            AppendInteger(branch_tag, i);
            branch_tag += ':';
            branch_tag += branch_predicate->ToString();
            const GroupId branch_group =
                memo.EnsureDerivedGroup(input_relations, branch_tag);
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

    // distinct_eager_over_join : Distinct(Join(L, R)) gains
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
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (distinct_eager_over_join_sound,
    //    cpp_distinct_eager_self_eq_uniqueness_bug)
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
            const std::vector<Expression> join_conjuncts =
                SplitConjuncts(join_predicate);
            for (int side = 0; side < side_limit; ++side) {
              const GroupId pushed_id = side == 0 ? left_id : right_id;
              const GroupId other_id = side == 0 ? right_id : left_id;
              const std::string pushed_rel =
                  side == 0 ? left_relations.front() : right_relations.front();
              const std::string other_rel =
                  side == 0 ? right_relations.front() : left_relations.front();
              std::unordered_set<std::string> other_join_cols;
              other_join_cols.reserve(join_conjuncts.size() * 2);
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
                const ColumnName& lhs_col =
                    binary.Left()->AsColumnValue().GetColumnName();
                const ColumnName& rhs_col =
                    binary.Right()->AsColumnValue().GetColumnName();
                if (lhs_col.schema == pushed_rel &&
                    rhs_col.schema == other_rel) {
                  InsertColumnMatcherNames(other_join_cols, rhs_col);
                } else if (lhs_col.schema == other_rel &&
                           rhs_col.schema == pushed_rel) {
                  InsertColumnMatcherNames(other_join_cols, lhs_col);
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
              std::string distinct_tag = "distinct_eager:";
              distinct_tag += pushed_rel;
              distinct_tag.push_back(':');
              AppendInteger(distinct_tag, pushed_id);
              const GroupId pushed_distinct =
                  memo.EnsureDerivedGroup(pushed_relations, distinct_tag);
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
              // The tag must identify the CHILDREN as well: with only the
              // relation names and predicate, two joins with different child
              // groups (e.g. one carrying an extra filter) share one derived
              // group and mix each other's alternatives.
              const GroupId pushed_join_group = memo.EnsureDerivedGroup(
                  input_relations, "distinct_eager_join:" + pushed_rel + ":" +
                                       join_predicate->ToString() + ":" +
                                       GroupIdsTag(pushed_join.children));
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

    // topn_push_through_union_all (the TopN pushdown contract): TopN(keys,
    // limit, offset) over UnionAll gains a UnionAll(TopN(keys,
    // limit+offset)...) alternative keeping the parent TopN. Every branch's
    // top-(limit+offset) contains all its rows that can appear in the global
    // top, so the parent re-establishes the exact output (D6: the parent TopN
    // keeps establishing the order). WITH TIES is excluded (tied peers overflow
    // the pushed cap). Branch tags carry the branch GroupId, ordering
    // signature, and union fingerprint so different UNION ALLs never share a
    // capped branch group (D1).
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (topn_push_through_union_all_sound)
    built.Add(Rule(
        "topn_push_through_union_all", TopN(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kTopN ||
              expression.children.size() != 1 || expression.with_ties ||
              expression.prior_limit_offset != 0) {
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
            AppendInteger(topn_key_signature, key.name.size());
            topn_key_signature.push_back(':');
            topn_key_signature.append(key.name);
            topn_key_signature.push_back('=');
            topn_key_signature.append(
                key.expression ? key.expression->ToString() : "<null>");
            topn_key_signature.push_back('|');
          }
          AppendInteger(topn_key_signature, topn_ascending.size());
          topn_key_signature.push_back(':');
          for (const bool ascending : topn_ascending) {
            topn_key_signature.push_back(ascending ? '1' : '0');
          }
          topn_key_signature.push_back('/');
          AppendInteger(topn_key_signature, topn_nulls_first.size());
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
              std::string child_tag = "topn_union_child:";
              AppendInteger(child_tag, child_id);
              child_tag += ':';
              AppendInteger(child_tag, total_limit);
              child_tag += ':';
              child_tag += topn_key_signature;
              const GroupId capped = memo.EnsureDerivedGroup(
                  memo.Get(child_id).relations, child_tag);
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
            std::string pushed_tag = "topn_union_pushed:";
            AppendInteger(pushed_tag, total_limit);
            pushed_tag.push_back(':');
            pushed_tag += topn_key_signature;
            pushed_tag.push_back(':');
            pushed_tag += union_expr.Fingerprint();
            const GroupId capped_union_group =
                memo.EnsureDerivedGroup(input_relations, pushed_tag);
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
    // partition_by payload, so mixed specs are split into maximal CONTIGUOUS
    // runs of identical partition specs: non-contiguous grouping (e.g.
    // [w0(p), w1(q), w2(p)] -> [w0, w2] + [w1]) would permute output columns
    // inside the same Memo group.
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (window_stack_and_merge_sound,
    //    cpp_split_window_noncontiguous_swaps_columns_bug)
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
          std::vector<std::pair<std::string, std::vector<size_t>>> runs;
          runs.reserve(expression.target_list.size());
          for (size_t i = 0; i < expression.target_list.size(); ++i) {
            const NamedExpression& target = expression.target_list[i];
            if (!target.expression ||
                target.expression->Type() != TypeTag::kWindowFunctionExp) {
              return;
            }
            const auto& call = dynamic_cast<
                const WindowFunctionCallExpression&>(  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
                *target.expression);
            std::string spec;
            AppendInteger(spec, call.partition_by.size());
            spec.push_back(':');
            for (const Expression& part : call.partition_by) {
              const std::string text = part ? part->ToString() : "<null>";
              AppendInteger(spec, text.size());
              spec.push_back('/');
              spec.append(text);
              spec.push_back(';');
            }
            if (runs.empty() || runs.back().first != spec) {
              runs.emplace_back(std::move(spec), std::vector<size_t>{i});
            } else {
              runs.back().second.push_back(i);
            }
          }
          if (runs.size() < 2) {
            return;
          }
          // Copy by value: EnsureDerivedGroup below may reallocate
          // memo.groups_.
          const std::vector<std::string> relations =
              memo.Get(input_id).relations;
          GroupId current = input_id;
          for (size_t s = 0; s < runs.size(); ++s) {
            const bool outermost = (s + 1 == runs.size());
            std::vector<NamedExpression> level_targets;
            level_targets.reserve(runs[s].second.size());
            std::vector<Expression> level_partition;
            for (size_t index : runs[s].second) {
              level_targets.push_back(expression.target_list[index]);
              if (level_partition.empty()) {
                const auto& call = dynamic_cast<
                    const WindowFunctionCallExpression&>(  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (unnest_and_values_rules_sound)
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
    // proof the back/round_trip expressions reproduce const, so
    // the boundary value survives the domain change exactly; (3) totality of
    // the allowlisted maps, so no row can throw in the original and vanish
    // in the rewrite. Only single-relation selections are rewritten; joins
    // keep their conjuncts for NewJoin canonicalization.
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (cast_pushdown_on_comparison_sound, cpp_parameter_slot_freeze_bug)
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
                  *const_side && !ContainsParameterSlot(*const_side)) {
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
    // Lean proof: lean/TinylambOpt/WindowTopNDecorrelate.lean
    //   (extract_year_sargable_sound, cpp_parameter_slot_freeze_bug)
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
                call.Args()[0]->Type() != TypeTag::kColumnValue ||
                ContainsParameterSlot(*const_side)) {
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
            // Bound direction table (lower = year-01-01, upper =
            // (year+1)-01-01):
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
            // Strengthen the group that owns this Selection, not the shared
            // base scan group: with the gate above, a branch-local Selection
            // must not remove rows another UNION/outer-join branch needs.
            // push_selection_into_scan copies the scan alternative into this
            // group, so the range still reaches the access path.
            if (memo.Get(group).relations.size() == 1) {
              memo.MergeScanFilter(group, CombineConjuncts(bounds));
            }
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
    // Lean proof: lean/TinylambOpt/ThreeValued.lean
    //   (self_eq_unsound_without_not_null, self_eq_sound_when_not_null,
    //    not_comparison_unsound_on_nan)
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

    // distinct_idempotent_aggregate_elimination: MIN/MAX/LOGICAL_AND/
    // LOGICAL_OR/BIT_AND/BIT_OR are idempotent commutative folds, so DISTINCT
    // without NeedsGroupContext() is an exact no-op (even with DOUBLE NaN
    // poisoning). Stripping DISTINCT unlocks HashAggregate merging, OOC partial
    // spill, and aggregate_union_transpose / eager_aggregation_over_join.
    // Lean proof: lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
    //   (min_max_distinct_elim_sound)
    built.Add(Rule(
        "distinct_idempotent_aggregate_elimination", Aggregation(Any("input")),
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
          bool changed = false;
          std::vector<NamedExpression> updated_targets;
          updated_targets.reserve(expression.target_list.size());
          for (const NamedExpression& target : expression.target_list) {
            NamedExpression copy = target;
            copy.expression =
                StripRedundantDistinctInExpr(target.expression, &changed);
            updated_targets.push_back(std::move(copy));
          }
          std::optional<Expression> updated_pred = expression.predicate;
          if (updated_pred && *updated_pred) {
            updated_pred =
                StripRedundantDistinctInExpr(*updated_pred, &changed);
          }
          if (!changed) {
            return;
          }
          LogicalExpression rewritten = expression;
          rewritten.target_list = std::move(updated_targets);
          rewritten.predicate = std::move(updated_pred);
          memo.AddExpression(group, std::move(rewritten));
        },
        LogicalOperator::kAggregation));

    // inner_join_to_semijoin_idempotent_aggs (Lean Theorem 3-3,
    // AvgMinMaxHavingDistinct.lean): When an Aggregation sits over Join(L, R,
    // p) and all grouping keys + projected expressions touch strictly one side
    // S while every aggregate is multiplicity-insensitive (MIN, MAX, LOGICAL_*,
    // BIT_AND, BIT_OR, or COUNT/SUM/AVG(DISTINCT)), duplicate matches from the
    // other side cannot change any group's existence or aggregate value, so
    // Join(L, R, p) reduces to SemiJoin(S, other, p).
    // Lean proof: lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
    //   (inner_join_to_semijoin_idempotent_aggs_sound,
    //    avg_skewed_fanout_breaks_semijoin_bug)
    built.Add(Rule(
        "inner_join_to_semijoin_idempotent_aggs", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1 ||
              expression.target_list.empty()) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group ||
              memo.Get(input_id).tag.starts_with("idempotent_agg_semi:")) {
            return;
          }
          if (std::ranges::any_of(memo.Get(input_id).expressions,
                                  [](const LogicalExpression& cand) {
                                    return cand.operation ==
                                           LogicalOperator::kSemiJoin;
                                  })) {
            return;
          }
          bool ok = true;
          std::vector<const AggregateExpression*> aggs;
          std::unordered_set<std::string> touched_schemas;
          const auto record_cols = [&](const Expression& e) {
            if (!e) {
              return;
            }
            for (const ColumnName& col : e->TouchedColumns()) {
              if (col.schema.empty() || col.name == "*") {
                ok = false;
                return;
              }
              touched_schemas.insert(col.schema);
            }
          };
          for (const Expression& key : expression.grouping_sets) {
            CollectAggregatesAndValidate(key, &aggs, &ok);
            record_cols(key);
          }
          for (const Expression& key : expression.partition_by) {
            CollectAggregatesAndValidate(key, &aggs, &ok);
            record_cols(key);
          }
          for (const NamedExpression& target : expression.target_list) {
            if (!target.expression) {
              return;
            }
            CollectAggregatesAndValidate(target.expression, &aggs, &ok);
            record_cols(target.expression);
          }
          if (expression.predicate && *expression.predicate) {
            CollectAggregatesAndValidate(*expression.predicate, &aggs, &ok);
            record_cols(*expression.predicate);
          }
          if (!ok || aggs.empty() || touched_schemas.empty()) {
            return;
          }
          if (!std::ranges::all_of(aggs, [](const AggregateExpression* agg) {
                return IsMultiplicityInsensitiveAggregate(*agg);
              })) {
            return;
          }

          const LogicalExpression& agg_copy = expression;
          const std::vector<LogicalExpression> join_candidates =
              memo.Get(input_id).expressions;
          const std::vector<std::string> input_relations =
              memo.Get(input_id).relations;

          for (const LogicalExpression& join_expr : join_candidates) {
            if (join_expr.operation != LogicalOperator::kJoin ||
                join_expr.children.size() != 2 || !join_expr.predicate ||
                !*join_expr.predicate) {
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            const auto& left_vec = memo.Get(left_id).relations;
            const auto& right_vec = memo.Get(right_id).relations;
            const std::unordered_set<std::string> left_rels(left_vec.begin(),
                                                            left_vec.end());
            const std::unordered_set<std::string> right_rels(right_vec.begin(),
                                                             right_vec.end());
            const bool only_left = std::ranges::all_of(
                touched_schemas,
                [&](const std::string& s) { return left_rels.contains(s); });
            const bool only_right = std::ranges::all_of(
                touched_schemas,
                [&](const std::string& s) { return right_rels.contains(s); });
            if (only_left == only_right) {
              continue;
            }
            const GroupId survivor_id = only_left ? left_id : right_id;
            const GroupId other_id = only_left ? right_id : left_id;
            const GroupId semi_group = memo.EnsureDerivedGroup(
                input_relations,
                "idempotent_agg_semi:" + GroupIdsTag({survivor_id, other_id}) +
                    PredicateTag(join_expr.predicate));
            InheritBranchLocal(memo, semi_group, group);
            if (semi_group == group || semi_group == input_id) {
              continue;
            }
            memo.AddExpression(
                semi_group,
                LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                  .children = {survivor_id, other_id},
                                  .predicate = join_expr.predicate});
            LogicalExpression over_semi = agg_copy;
            over_semi.children = {semi_group};
            memo.AddExpression(group, std::move(over_semi));
          }
        },
        LogicalOperator::kAggregation));

    // having_extremum_bound_pushdown (Lean Theorems 4-2 & 4-2',
    // AvgMinMaxHavingDistinct.lean): When HAVING filters by MAX(col) > c / >= c
    // (resp. MIN(col) < c / <= c) and that single MAX(col) (resp. MIN(col)) is
    // the ONLY aggregate in the query, rows failing col > c / >= c (resp.
    // col < c / <= c) can never witness or change a surviving group's extremum.
    // Restricted to non-DOUBLE reflexive domains (INT64, DATE, VARCHAR) because
    // GoogleSQL DOUBLE MIN/MAX propagates NaN across the group. HAVING stays
    // intact above Aggregation so scalar empty-input semantics stay exact.
    // Lean proof: lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
    //   (having_max_gt_pushdown_to_where_sound,
    //    having_min_lt_pushdown_to_where_sound,
    //    having_min_gt_pushed_to_where_bug,
    //    having_max_gt_breaks_sibling_avg_bug)
    built.Add(Rule(
        "having_extremum_bound_pushdown", Selection(Aggregation(Any(), "agg")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId agg_id = bindings.at("agg");
          if (agg_id == group ||
              memo.Get(agg_id).tag.starts_with("agg-having-extremum-pushed:")) {
            return;
          }
          const Expression having_pred = *expression.predicate;
          const std::vector<NamedExpression> sel_targets =
              expression.target_list;
          const Schema sel_schema = expression.output_schema;
          const std::vector<std::string> group_relations =
              memo.Get(group).relations;
          const std::vector<LogicalExpression> agg_candidates =
              memo.Get(agg_id).expressions;

          for (const LogicalExpression& aggregation : agg_candidates) {
            if (aggregation.operation != LogicalOperator::kAggregation ||
                aggregation.children.size() != 1 ||
                (aggregation.predicate && *aggregation.predicate)) {
              continue;
            }
            const GroupId input_id = aggregation.children[0];
            if (input_id == group || input_id == agg_id ||
                memo.Get(input_id).tag.starts_with("sel-having-extremum:")) {
              continue;
            }
            std::optional<Expression> resolved_having =
                ResolveHavingThroughOutputs(having_pred,
                                            aggregation.target_list);
            if (!resolved_having || !*resolved_having) {
              continue;
            }
            bool ok = true;
            std::vector<const AggregateExpression*> aggs;
            for (const NamedExpression& target : aggregation.target_list) {
              CollectAggregatesAndValidate(target.expression, &aggs, &ok);
            }
            CollectAggregatesAndValidate(*resolved_having, &aggs, &ok);
            if (!ok || aggs.empty()) {
              continue;
            }
            const AggregationType ext_type = aggs.front()->GetType();
            if (ext_type != AggregationType::kMax &&
                ext_type != AggregationType::kMin) {
              continue;
            }
            if (!aggs.front()->Child() ||
                aggs.front()->Child()->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName ext_col =
                aggs.front()->Child()->AsColumnValue().GetColumnName();
            const std::optional<ValueType> col_domain =
                ResolveMemoColumnType(memo, ext_col);
            if (!col_domain || (*col_domain != ValueType::kInt64 &&
                                *col_domain != ValueType::kDate &&
                                *col_domain != ValueType::kVarChar)) {
              continue;
            }
            const bool single_clean_extremum =
                std::ranges::all_of(aggs, [&](const AggregateExpression* agg) {
                  return agg->GetType() == ext_type &&
                         !agg->NeedsGroupContext() && !agg->HasWhereFilter() &&
                         agg->InnerGroupBy().empty() && agg->Child() &&
                         agg->Child()->Type() == TypeTag::kColumnValue &&
                         agg->Child()->AsColumnValue().GetColumnName() ==
                             ext_col;
                });
            if (!single_clean_extremum) {
              continue;
            }

            std::vector<Expression> pushed_bounds;
            for (const Expression& conj : SplitConjuncts(*resolved_having)) {
              if (!conj || conj->Type() != TypeTag::kBinaryExp) {
                continue;
              }
              const auto& bin = conj->AsBinaryExpression();
              BinaryOperation op = bin.Op();
              Expression lhs = bin.Left();
              Expression rhs = bin.Right();
              if (lhs && rhs && lhs->Type() == TypeTag::kConstantValue &&
                  rhs->Type() == TypeTag::kAggregateExp) {
                switch (op) {
                  case BinaryOperation::kLessThan:
                    op = BinaryOperation::kGreaterThan;
                    std::swap(lhs, rhs);
                    break;
                  case BinaryOperation::kLessThanEquals:
                    op = BinaryOperation::kGreaterThanEquals;
                    std::swap(lhs, rhs);
                    break;
                  case BinaryOperation::kGreaterThan:
                    op = BinaryOperation::kLessThan;
                    std::swap(lhs, rhs);
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    op = BinaryOperation::kLessThanEquals;
                    std::swap(lhs, rhs);
                    break;
                  default:
                    break;
                }
              }
              if (!lhs || !rhs || lhs->Type() != TypeTag::kAggregateExp ||
                  rhs->Type() != TypeTag::kConstantValue ||
                  ContainsParameterSlot(rhs)) {
                continue;
              }
              const Value& cval = rhs->AsConstantValue().GetValue();
              if (cval.IsNull() || cval.type != *col_domain) {
                continue;
              }
              if (ext_type == AggregationType::kMax &&
                  (op == BinaryOperation::kGreaterThan ||
                   // NOLINTNEXTLINE(bugprone-branch-clone) distinct intent in
                   // each branch
                   op == BinaryOperation::kGreaterThanEquals)) {
                pushed_bounds.push_back(
                    BinaryExpressionExp(ColumnValueExp(ext_col), op, rhs));
              } else if (ext_type == AggregationType::kMin &&
                         (op == BinaryOperation::kLessThan ||
                          op == BinaryOperation::kLessThanEquals)) {
                pushed_bounds.push_back(
                    BinaryExpressionExp(ColumnValueExp(ext_col), op, rhs));
              }
            }
            if (pushed_bounds.empty()) {
              continue;
            }

            const Expression combined_pushed =
                CanonicalizeConjuncts(CombineConjuncts(pushed_bounds));
            const std::vector<std::string> input_relations =
                memo.Get(input_id).relations;
            const GroupId filtered = memo.EnsureDerivedGroup(
                input_relations,
                "sel-having-extremum:" + GroupIdsTag({input_id}) +
                    combined_pushed->ToString());
            InheritBranchLocal(memo, filtered, group);
            if (filtered == group || filtered == agg_id ||
                filtered == input_id) {
              continue;
            }
            memo.AddExpression(
                filtered,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {input_id},
                                  .predicate = combined_pushed});
            const GroupId new_agg = memo.EnsureDerivedGroup(
                group_relations,
                "agg-having-extremum-pushed:" + GroupIdsTag({filtered}) +
                    TargetListFingerprint(aggregation.target_list));
            InheritBranchLocal(memo, new_agg, group);
            if (new_agg == group || new_agg == filtered) {
              continue;
            }
            LogicalExpression pushed_agg = aggregation;
            pushed_agg.children = {filtered};
            memo.AddExpression(new_agg, std::move(pushed_agg));
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {new_agg},
                                  .predicate = having_pred,
                                  .target_list = sel_targets,
                                  .output_schema = sel_schema});
          }
        },
        LogicalOperator::kSelection));

    // outer_to_inner_join_on_null_rejecting_having (Lean Theorem 4-3,
    // AvgMinMaxHavingDistinct.lean): Demotes LeftOuterJoin(L, R, p) below
    // Aggregation + Selection(HAVING) to Join(L, R, p) when GROUP BY and
    // non-aggregate outputs do not touch R, every aggregate is a non-COUNT(*)
    // SUM/AVG/MIN/MAX/COUNT(r_expr) strict on R (so unmatched NULL-padded rows
    // contribute nothing to mixed groups), and HAVING rejects groups whose
    // aggregates all evaluate to their unmatched identity (NULL / 0).
    // Lean proof: lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
    //   (having_null_rejecting_demotes_left_join_sound,
    //    having_demote_breaks_count_star_on_coarse_group)
    built.Add(Rule(
        "outer_to_inner_join_on_null_rejecting_having",
        Selection(Aggregation(Any(), "agg")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId agg_id = bindings.at("agg");
          if (agg_id == group ||
              memo.Get(agg_id).tag.starts_with("agg-over-demoted-outer:")) {
            return;
          }
          const Expression having_pred = *expression.predicate;
          const std::vector<NamedExpression> sel_targets =
              expression.target_list;
          const Schema sel_schema = expression.output_schema;
          const std::vector<std::string> group_relations =
              memo.Get(group).relations;
          const std::vector<LogicalExpression> agg_candidates =
              memo.Get(agg_id).expressions;

          for (const LogicalExpression& aggregation : agg_candidates) {
            if (aggregation.operation != LogicalOperator::kAggregation ||
                aggregation.children.size() != 1 ||
                (aggregation.predicate && *aggregation.predicate)) {
              continue;
            }
            const GroupId join_group_id = aggregation.children[0];
            if (join_group_id == group || join_group_id == agg_id ||
                memo.Get(join_group_id)
                    .tag.starts_with("demoted_outer_having:")) {
              continue;
            }
            std::optional<Expression> resolved_having =
                ResolveHavingThroughOutputs(having_pred,
                                            aggregation.target_list);
            if (!resolved_having || !*resolved_having) {
              continue;
            }
            bool ok = true;
            std::vector<const AggregateExpression*> aggs;
            std::vector<ColumnName> bare_cols;
            for (const Expression& key : aggregation.grouping_sets) {
              CollectAggregatesAndValidate(key, &aggs, &ok);
              CollectNonAggregateColumns(key, &bare_cols, &ok);
            }
            for (const Expression& key : aggregation.partition_by) {
              CollectAggregatesAndValidate(key, &aggs, &ok);
              CollectNonAggregateColumns(key, &bare_cols, &ok);
            }
            for (const NamedExpression& target : aggregation.target_list) {
              CollectAggregatesAndValidate(target.expression, &aggs, &ok);
              CollectNonAggregateColumns(target.expression, &bare_cols, &ok);
            }
            CollectAggregatesAndValidate(*resolved_having, &aggs, &ok);
            CollectNonAggregateColumns(*resolved_having, &bare_cols, &ok);
            if (!ok || aggs.empty()) {
              continue;
            }

            const std::vector<LogicalExpression> join_candidates =
                memo.Get(join_group_id).expressions;
            const std::vector<std::string> join_relations =
                memo.Get(join_group_id).relations;
            std::optional<std::string> target_fingerprint;
            for (const LogicalExpression& join_expr : join_candidates) {
              if (join_expr.operation != LogicalOperator::kOuterJoin ||
                  join_expr.children.size() != 2 ||
                  join_expr.join_type != 0) {  // 0 = LeftOuter
                continue;
              }
              const GroupId left_id = join_expr.children[0];
              const GroupId right_id = join_expr.children[1];
              const std::vector<std::string> right_rels_vec =
                  memo.Get(right_id).relations;
              const std::unordered_set<std::string> right_rels(
                  right_rels_vec.begin(), right_rels_vec.end());
              if (right_rels.empty()) {
                continue;
              }
              const bool bare_touches_right =
                  std::ranges::any_of(bare_cols, [&](const ColumnName& c) {
                    return c.schema.empty() || c.name == "*" ||
                           right_rels.contains(c.schema);
                  });
              if (bare_touches_right) {
                continue;
              }
              const bool all_aggs_strict_on_right = std::ranges::all_of(
                  aggs, [&](const AggregateExpression* agg) {
                    if (agg->NeedsGroupContext() || agg->HasWhereFilter() ||
                        !agg->InnerGroupBy().empty() || !agg->Child()) {
                      return false;
                    }
                    switch (agg->GetType()) {
                      case AggregationType::kSum:
                      case AggregationType::kAvg:
                      case AggregationType::kMin:
                      case AggregationType::kMax:
                      case AggregationType::kCount:
                        break;
                      default:
                        return false;
                    }
                    return ExpressionCannotThrowForPlanner(agg->Child()) &&
                           IsStrictOnRelations(agg->Child(), right_rels_vec);
                  });
              if (!all_aggs_strict_on_right) {
                continue;
              }
              const auto having_conjuncts = SplitConjuncts(*resolved_having);
              if (!std::ranges::any_of(having_conjuncts,
                                       HavingConjunctRejectsUnmatchedGroup)) {
                continue;
              }

              const GroupId inner_join_group = memo.EnsureDerivedGroup(
                  join_relations,
                  "demoted_outer_having:" + GroupIdsTag({left_id, right_id}) +
                      PredicateTag(join_expr.predicate));
              InheritBranchLocal(memo, inner_join_group, group);
              if (inner_join_group == group || inner_join_group == agg_id ||
                  inner_join_group == join_group_id) {
                continue;
              }
              const bool has_pred = join_expr.predicate && *join_expr.predicate;
              memo.AddExpression(
                  inner_join_group,
                  LogicalExpression{
                      .operation = has_pred ? LogicalOperator::kJoin
                                            : LogicalOperator::kCrossJoin,
                      .children = {left_id, right_id},
                      .predicate =
                          has_pred ? join_expr.predicate : std::nullopt});
              if (!target_fingerprint) {
                target_fingerprint =
                    TargetListFingerprint(aggregation.target_list);
              }
              const GroupId new_agg = memo.EnsureDerivedGroup(
                  group_relations,
                  "agg-over-demoted-outer:" + GroupIdsTag({inner_join_group}) +
                      *target_fingerprint);
              InheritBranchLocal(memo, new_agg, group);
              if (new_agg == group || new_agg == inner_join_group) {
                continue;
              }
              LogicalExpression agg_over_inner = aggregation;
              agg_over_inner.children = {inner_join_group};
              memo.AddExpression(new_agg, std::move(agg_over_inner));
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {new_agg},
                                    .predicate = having_pred,
                                    .target_list = sel_targets,
                                    .output_schema = sel_schema});
            }
          }
        },
        LogicalOperator::kSelection));

    // eager_aggregation_over_join (the relevant Lean theorems,
    // AggJoinDiscovery.lean & AvgMinMaxHavingDistinct.lean): Push a 2-stage
    // partial Aggregation below an inner equi-join Join(L, R, equi_pred) onto
    // side S when all aggregates (COUNT, MIN, MAX) depend exclusively on S
    // and GROUP BY is non-empty. Pre-aggregates S by (outer_keys_in_S ∪
    // join_keys_in_S), joins the collapsed table with the other side, and
    // finishes with final SUM/MIN/MAX above the join (COUNT -> SUM(partial)).
    // Cascades across multi-hop star/snowflake joins automatically.
    // Lean proof: lean/TinylambOpt/AggJoinDiscovery.lean
    //   (eager_group_agg_over_mn_join_sound,
    //   eager_scalar_agg_over_mn_join_sound,
    //    star_cascade_two_hop_agg_sound) &
    //   lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
    //   (eager_avg_min_max_over_mn_join_sound)
    built.Add(Rule(
        "eager_aggregation_over_join", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1 ||
              expression.target_list.empty() ||
              (expression.predicate && *expression.predicate)) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group ||
              memo.Get(group).tag.starts_with("eager_pre_agg:") ||
              memo.Get(input_id).tag.starts_with("eager_agg_join:")) {
            return;
          }
          const std::vector<Expression> outer_keys =
              expression.grouping_sets.empty() ? expression.partition_by
                                               : expression.grouping_sets;
          if (outer_keys.empty()) {
            return;
          }
          std::vector<ColumnName> outer_key_cols;
          outer_key_cols.reserve(outer_keys.size());
          for (const Expression& key : outer_keys) {
            if (!key || key->Type() != TypeTag::kColumnValue) {
              return;
            }
            const ColumnName& col = key->AsColumnValue().GetColumnName();
            if (col.schema.empty() || col.name.empty() || col.name == "*") {
              return;
            }
            outer_key_cols.push_back(col);
          }
          bool saw_aggregate = false;
          for (const NamedExpression& target : expression.target_list) {
            if (!target.expression) {
              return;
            }
            if (target.expression->Type() == TypeTag::kColumnValue) {
              const ColumnName& col =
                  target.expression->AsColumnValue().GetColumnName();
              if (std::ranges::find(outer_key_cols, col) ==
                  outer_key_cols.end()) {
                return;
              }
              continue;
            }
            if (target.expression->Type() != TypeTag::kAggregateExp) {
              return;
            }
            const auto& agg = target.expression->AsAggregateExpression();
            if (agg.Distinct() || agg.HasWhereFilter() ||
                agg.NeedsGroupContext() || !agg.InnerGroupBy().empty()) {
              return;
            }
            // A partial SUM can overflow on a key the join would discard.
            // Even a unique opposite key cannot rule this out (Lean:
            // BranchlessJitCodegen.eager_sum_unmatched_key_overflow).
            if (agg.GetType() != AggregationType::kCount &&
                agg.GetType() != AggregationType::kMin &&
                agg.GetType() != AggregationType::kMax) {
              return;
            }
            saw_aggregate = true;
          }
          if (!saw_aggregate) {
            return;
          }

          const LogicalExpression& orig_agg_expr = expression;
          const std::vector<LogicalExpression> join_candidates =
              memo.Get(input_id).expressions;
          const std::vector<std::string> input_relations =
              memo.Get(input_id).relations;

          for (const LogicalExpression& join_expr : join_candidates) {
            if (join_expr.operation != LogicalOperator::kJoin ||
                join_expr.children.size() != 2 || !join_expr.predicate ||
                !*join_expr.predicate) {
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            const std::vector<std::string> left_vec =
                memo.Get(left_id).relations;
            const std::vector<std::string> right_vec =
                memo.Get(right_id).relations;
            const std::unordered_set<std::string> left_rels(left_vec.begin(),
                                                            left_vec.end());
            const std::unordered_set<std::string> right_rels(right_vec.begin(),
                                                             right_vec.end());
            const auto join_conjuncts = SplitConjuncts(*join_expr.predicate);
            std::vector<std::pair<ColumnName, ColumnName>> equi_pairs;
            equi_pairs.reserve(join_conjuncts.size());
            bool all_equi = true;
            for (const Expression& conj : join_conjuncts) {
              if (!conj || conj->Type() != TypeTag::kBinaryExp ||
                  conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
                all_equi = false;
                break;
              }
              const Expression& lhs = conj->AsBinaryExpression().Left();
              const Expression& rhs = conj->AsBinaryExpression().Right();
              if (!lhs || !rhs || lhs->Type() != TypeTag::kColumnValue ||
                  rhs->Type() != TypeTag::kColumnValue) {
                all_equi = false;
                break;
              }
              const ColumnName& lc = lhs->AsColumnValue().GetColumnName();
              const ColumnName& rc = rhs->AsColumnValue().GetColumnName();
              if (lc.schema.empty() || rc.schema.empty()) {
                all_equi = false;
                break;
              }
              if (left_rels.contains(lc.schema) &&
                  right_rels.contains(rc.schema)) {
                equi_pairs.emplace_back(lc, rc);
              } else if (right_rels.contains(lc.schema) &&
                         left_rels.contains(rc.schema)) {
                equi_pairs.emplace_back(rc, lc);
              } else {
                all_equi = false;
                break;
              }
            }
            if (!all_equi || equi_pairs.empty()) {
              continue;
            }

            for (int side = 0; side < 2; ++side) {
              const GroupId side_id = side == 0 ? left_id : right_id;
              if (memo.Get(side_id).tag.starts_with("eager_pre_agg:")) {
                continue;
              }
              const std::unordered_set<std::string>& side_rels =
                  side == 0 ? left_rels : right_rels;
              const std::vector<std::string>& side_relations =
                  side == 0 ? left_vec : right_vec;

              // Check all aggregates depend strictly on side_rels (COUNT(*) has
              // 0 touched columns and is valid on either side).
              bool aggs_on_side = true;
              for (const NamedExpression& target : orig_agg_expr.target_list) {
                // The rule's entry guard rejects null target expressions;
                // keep the check local so this loop is safe on its own.
                if (!target.expression) {
                  aggs_on_side = false;
                  break;
                }
                if (target.expression->Type() != TypeTag::kAggregateExp) {
                  continue;
                }
                for (const ColumnName& col :
                     target.expression->TouchedColumns()) {
                  if (col.name == "*") {
                    continue;
                  }
                  if (col.schema.empty() || !side_rels.contains(col.schema)) {
                    aggs_on_side = false;
                    break;
                  }
                }
                if (!aggs_on_side) {
                  break;
                }
              }
              if (!aggs_on_side) {
                continue;
              }

              // Build pre-aggregation keys: outer_keys in side ∪ join_keys in
              // side.
              std::vector<ColumnName> pre_key_cols;
              pre_key_cols.reserve(outer_key_cols.size() + equi_pairs.size());
              auto add_pre_key = [&](const ColumnName& c) {
                if (std::ranges::find(pre_key_cols, c) == pre_key_cols.end()) {
                  pre_key_cols.push_back(c);
                }
              };
              bool all_outer_keys_on_side = true;
              for (const ColumnName& c : outer_key_cols) {
                if (!side_rels.contains(c.schema)) {
                  // A GROUP BY key spanning both sides cannot be represented
                  // by a single side's pre-aggregation key: silently dropping
                  // the other side would coarsen the GROUP BY granularity.
                  all_outer_keys_on_side = false;
                  break;
                }
                add_pre_key(c);
              }
              if (!all_outer_keys_on_side) {
                continue;
              }
              for (const auto& [lc, rc] : equi_pairs) {
                add_pre_key(side == 0 ? lc : rc);
              }
              if (pre_key_cols.empty()) {
                continue;
              }
              // Guard against unqualified `.name` collisions in
              // AggregationPlan's output schema, and skip pre-aggregation when
              // pre_key_cols already covers a candidate key of side_id (1:1
              // no-op).
              std::unordered_set<std::string> seen_unqualified_names;
              std::unordered_set<std::string> pre_key_strs;
              seen_unqualified_names.reserve(pre_key_cols.size());
              pre_key_strs.reserve(pre_key_cols.size());
              bool name_collision = false;
              for (const ColumnName& c : pre_key_cols) {
                if (!seen_unqualified_names.insert(c.name).second) {
                  name_collision = true;
                  break;
                }
                pre_key_strs.insert(c.ToString());
              }
              if (name_collision) {
                continue;
              }
              const auto& side_cks =
                  memo.Get(side_id).logical_properties.candidate_keys;
              const bool side_already_unique = std::ranges::any_of(
                  side_cks, [&](const std::unordered_set<std::string>& ck) {
                    return !ck.empty() &&
                           std::ranges::all_of(ck, [&](const std::string& k) {
                             return pre_key_strs.contains(k);
                           });
                  });
              if (side_already_unique) {
                continue;
              }

              std::vector<Expression> pre_grouping_exprs;
              pre_grouping_exprs.reserve(pre_key_cols.size());
              for (const ColumnName& c : pre_key_cols) {
                pre_grouping_exprs.push_back(ColumnValueExp(c));
              }
              std::vector<NamedExpression> pre_targets;
              pre_targets.reserve(orig_agg_expr.target_list.size());
              std::vector<NamedExpression> final_targets;
              final_targets.reserve(orig_agg_expr.target_list.size());
              size_t partial_idx = 0;
              bool valid_targets = true;
              for (const NamedExpression& target : orig_agg_expr.target_list) {
                if (!target.expression) {
                  // Defensive: the entry guard rejects partially built
                  // aggregation nodes, but never dereference an unverified
                  // target expression here.
                  valid_targets = false;
                  break;
                }
                if (target.expression->Type() != TypeTag::kAggregateExp) {
                  final_targets.push_back(target);
                  continue;
                }
                const auto& orig = target.expression->AsAggregateExpression();
                std::string synth_name = "$pre_agg_";
                AppendInteger(synth_name, partial_idx++);
                pre_targets.emplace_back(synth_name,
                                         CloneAggregate(orig, false));
                const AggregationType final_op =
                    orig.GetType() == AggregationType::kCount
                        ? AggregationType::kSum
                        : orig.GetType();
                auto final_agg = std::make_shared<AggregateExpression>(
                    final_op, ColumnValueExp(ColumnName("", synth_name)),
                    false);
                if (!orig.ArrayElementSqlType().empty()) {
                  final_agg->SetArrayElementSqlType(orig.ArrayElementSqlType());
                }
                final_targets.emplace_back(target.name, std::move(final_agg));
              }
              if (!valid_targets) {
                continue;
              }

              // Filter NULL join keys before pre-aggregating when not already
              // proven NOT NULL (Lean: innerJoin_filter_left_isSome).
              GroupId pre_input_id = side_id;
              std::vector<Expression> nn_filters;
              nn_filters.reserve(equi_pairs.size());
              for (const auto& [lc, rc] : equi_pairs) {
                const ColumnName& side_join_col = side == 0 ? lc : rc;
                if (!memo.Get(side_id).logical_properties.IsNotNull(
                        side_join_col.ToString())) {
                  nn_filters.push_back(
                      UnaryExpressionExp(ColumnValueExp(side_join_col),
                                         UnaryOperation::kIsNotNull));
                }
              }
              if (!nn_filters.empty()) {
                const Expression nn_pred =
                    CanonicalizeConjuncts(CombineConjuncts(nn_filters));
                const GroupId nn_group = memo.EnsureDerivedGroup(
                    side_relations,
                    "eager_nn_filter:" + GroupIdsTag({side_id}) +
                        nn_pred->ToString());
                InheritBranchLocal(memo, nn_group, group);
                if (nn_group != group && nn_group != side_id) {
                  memo.AddExpression(
                      nn_group, LogicalExpression{
                                    .operation = LogicalOperator::kSelection,
                                    .children = {side_id},
                                    .predicate = nn_pred});
                  pre_input_id = nn_group;
                }
              }

              std::string pre_key_tag;
              for (const ColumnName& c : pre_key_cols) {
                pre_key_tag += c.ToString() + ",";
              }
              const GroupId pre_agg_group = memo.EnsureDerivedGroup(
                  side_relations,
                  "eager_pre_agg:" + GroupIdsTag({pre_input_id}) + pre_key_tag +
                      TargetListFingerprint(pre_targets));
              InheritBranchLocal(memo, pre_agg_group, group);
              if (pre_agg_group == group || pre_agg_group == input_id ||
                  pre_agg_group == side_id) {
                continue;
              }
              memo.AddExpression(
                  pre_agg_group,
                  LogicalExpression{
                      .operation = LogicalOperator::kAggregation,
                      .children = {pre_input_id},
                      .target_list = std::move(pre_targets),
                      .grouping_sets = std::move(pre_grouping_exprs)});

              const GroupId new_left = side == 0 ? pre_agg_group : left_id;
              const GroupId new_right = side == 0 ? right_id : pre_agg_group;
              const GroupId post_join_group = memo.EnsureDerivedGroup(
                  input_relations,
                  "eager_agg_join:" + GroupIdsTag({new_left, new_right}) +
                      PredicateTag(join_expr.predicate));
              InheritBranchLocal(memo, post_join_group, group);
              if (post_join_group == group || post_join_group == input_id) {
                continue;
              }
              memo.AddExpression(
                  post_join_group,
                  LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {new_left, new_right},
                                    .predicate = join_expr.predicate});

              LogicalExpression final_agg_expr = orig_agg_expr;
              final_agg_expr.children = {post_join_group};
              final_agg_expr.target_list = std::move(final_targets);
              memo.AddExpression(group, std::move(final_agg_expr));
            }
          }
        },
        LogicalOperator::kAggregation));

    // disjoint_union_group_by_bypass: When GROUP BY over UNION ALL includes at
    // least one grouping column projected as pairwise-distinct non-NULL
    // binary-collated constants across all union branches (e.g. TPC-DS Q76
    // `'store'` / `'web'` / `'catalog'`), groups from different branches are
    // provably disjoint, so the full Aggregation (including AVG, COUNT
    // DISTINCT, compound expressions, and HAVING) pushes into each branch and
    // emits UNION ALL directly without any outer re-aggregation.
    // Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
    //   (disjoint_union_group_by_bypass_sound,
    //    agg_union_null_tag_collision_bug)
    built.Add(Rule(
        "disjoint_union_group_by_bypass", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1 ||
              expression.grouping_sets.empty() ||
              expression.target_list.empty()) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const std::string& input_tag = memo.Get(input_id).tag;
          if (input_tag.starts_with("union_all_pushed_aggs:") ||
              input_tag.starts_with("disjoint_union_agg_branch_")) {
            return;
          }
          std::vector<ColumnName> group_key_cols;
          for (const Expression& g : expression.grouping_sets) {
            if (g && g->Type() == TypeTag::kColumnValue) {
              group_key_cols.push_back(g->AsColumnValue().GetColumnName());
            }
          }
          if (group_key_cols.empty()) {
            return;
          }
          // Copy by value: EnsureDerivedGroup below may reallocate
          // memo.groups_.
          const LogicalExpression& orig = expression;
          const std::vector<LogicalExpression> union_candidates =
              memo.Get(input_id).expressions;

          const auto branch_constant_for_key =
              [&](GroupId branch_id,
                  const ColumnName& key) -> std::optional<Value> {
            for (const LogicalExpression& bexpr :
                 memo.Get(branch_id).expressions) {
              if (bexpr.operation != LogicalOperator::kProjection) {
                continue;
              }
              for (const NamedExpression& item : bexpr.target_list) {
                if (!item.expression ||
                    item.expression->Type() != TypeTag::kConstantValue ||
                    ContainsParameterSlot(item.expression)) {
                  continue;
                }
                const bool matches_name =
                    (!item.name.empty() &&
                     IdentifierEquals(item.name, key.name)) ||
                    (key.schema.empty() && !item.name.empty() &&
                     item.name == key.name);
                if (!matches_name) {
                  continue;
                }
                const Value& val =
                    item.expression->AsConstantValue().GetValue();
                if (val.IsNull() || val.Collation() != 0) {
                  continue;
                }
                if (val.type != ValueType::kVarChar &&
                    val.type != ValueType::kInt64 &&
                    val.type != ValueType::kDate) {
                  continue;
                }
                return val;
              }
            }
            return std::nullopt;
          };

          for (const LogicalExpression& union_expr : union_candidates) {
            if (union_expr.operation != LogicalOperator::kUnionAll ||
                union_expr.children.size() < 2) {
              continue;
            }
            bool has_disjoint_tag = false;
            for (const ColumnName& key : group_key_cols) {
              std::vector<Value> tags;
              tags.reserve(union_expr.children.size());
              bool all_const = true;
              for (const GroupId branch_id : union_expr.children) {
                std::optional<Value> tag =
                    branch_constant_for_key(branch_id, key);
                if (!tag.has_value() ||
                    (!tags.empty() && tag->type != tags.front().type)) {
                  all_const = false;
                  break;
                }
                tags.push_back(std::move(*tag));
              }
              if (!all_const) {
                continue;
              }
              bool pairwise_distinct = true;
              for (size_t a = 0; a < tags.size() && pairwise_distinct; ++a) {
                for (size_t b = a + 1; b < tags.size(); ++b) {
                  if (tags[a] == tags[b]) {
                    pairwise_distinct = false;
                    break;
                  }
                }
              }
              if (pairwise_distinct) {
                has_disjoint_tag = true;
                break;
              }
            }
            if (!has_disjoint_tag) {
              continue;
            }

            std::vector<GroupId> pushed_aggs;
            pushed_aggs.reserve(union_expr.children.size());
            const std::string fp = orig.Fingerprint();
            for (size_t i = 0; i < union_expr.children.size(); ++i) {
              const GroupId branch_id = union_expr.children[i];
              const std::vector<std::string> branch_rels =
                  memo.Get(branch_id).relations;
              const GroupId branch_agg = memo.EnsureDerivedGroup(
                  branch_rels, "disjoint_union_agg_branch_" +
                                   std::to_string(i) + ":" +
                                   std::to_string(branch_id) + ":" + fp);
              InheritBranchLocal(memo, branch_agg, group);
              if (branch_agg == branch_id || branch_agg == group) {
                break;
              }
              LogicalExpression branch_expr = orig;
              branch_expr.children = {branch_id};
              memo.AddExpression(branch_agg, std::move(branch_expr));
              pushed_aggs.push_back(branch_agg);
            }
            if (pushed_aggs.size() != union_expr.children.size()) {
              continue;
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                  .children = std::move(pushed_aggs),
                                  .target_list = orig.target_list,
                                  .output_schema = orig.output_schema});
          }
        },
        LogicalOperator::kAggregation));

    // outer_to_anti_join_through_inner_join: Convert
    // Selection(Join(LeftOuterJoin(A, B, pB), C, pC), b_col IS NULL AND resAC)
    // into Selection(AntiJoin(Join(A, C, pC), B, pB), resAC) when `pC` touches
    // only `A ∪ C` and `b_col` is provably non-NULL on every `pB` match (e.g.
    // TPC-DS Q78 `sales LEFT JOIN returns JOIN date_dim WHERE r_key IS NULL`).
    // Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
    //   (outer_join_intervening_inner_to_antijoin_sound)
    built.Add(Rule(
        "outer_to_anti_join_through_inner_join", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          // Bare root Selections whose positional output_schema retains dropped
          // right-side columns must keep LeftOuterJoin unless an explicit
          // target_list (or an enclosing Projection/Aggregation group) projects
          // the surviving columns.
          if (expression.output_schema.ColumnCount() > 0 &&
              expression.target_list.empty()) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group ||
              (memo.Get(input_id).tag.starts_with("outer_to_anti_base:") ||
               memo.Get(input_id).tag.starts_with(
                   "outer_to_anti_base_thru:"))) {
            return;
          }
          const std::vector<LogicalExpression> join_c_candidates =
              memo.Get(input_id).expressions;
          const std::vector<std::string> input_relations =
              memo.Get(input_id).relations;
          const Expression sel_predicate = *expression.predicate;
          const std::vector<NamedExpression> sel_targets =
              expression.target_list;
          const Schema sel_schema = expression.output_schema;

          for (const LogicalExpression& join_c : join_c_candidates) {
            if (join_c.operation != LogicalOperator::kJoin ||
                join_c.children.size() != 2) {
              continue;
            }
            for (int side = 0; side < 2; ++side) {
              const GroupId ojoin_id = join_c.children[side];
              const GroupId c_id = join_c.children[1 - side];
              const std::vector<LogicalExpression> ojoin_candidates =
                  memo.Get(ojoin_id).expressions;
              const std::vector<std::string> c_rels_vec =
                  memo.Get(c_id).relations;

              for (const LogicalExpression& ojoin : ojoin_candidates) {
                if (ojoin.operation != LogicalOperator::kOuterJoin ||
                    ojoin.children.size() != 2 || ojoin.join_type != 0) {
                  continue;
                }
                const GroupId a_id = ojoin.children[0];
                const GroupId b_id = ojoin.children[1];
                const std::vector<std::string> a_rels_vec =
                    memo.Get(a_id).relations;
                const std::vector<std::string> b_rels_vec =
                    memo.Get(b_id).relations;
                std::unordered_set<std::string> ac_rels(a_rels_vec.begin(),
                                                        a_rels_vec.end());
                ac_rels.insert(c_rels_vec.begin(), c_rels_vec.end());
                const std::unordered_set<std::string> b_rels(b_rels_vec.begin(),
                                                             b_rels_vec.end());

                if (join_c.predicate && *join_c.predicate) {
                  const Expression& p_c = *join_c.predicate;
                  if (ContainsAggregate(p_c) || TreeContainsWindowCall(p_c) ||
                      !SafeToReduceEvaluationCount(p_c)) {
                    continue;
                  }
                  const auto p_c_cols = p_c->TouchedColumns();
                  if (!std::ranges::all_of(p_c_cols, [&](const ColumnName& c) {
                        return !c.schema.empty() &&
                               ac_rels.contains(c.schema) &&
                               !b_rels.contains(c.schema);
                      })) {
                    continue;
                  }
                }

                size_t null_checks = 0;
                bool convertible = true;
                std::vector<Expression> ac_residuals;
                for (const Expression& conjunct :
                     SplitConjuncts(sel_predicate)) {
                  if (!conjunct) {
                    convertible = false;
                    break;
                  }
                  if (conjunct->Type() == TypeTag::kUnaryExp &&
                      conjunct->AsUnaryExpression().Op() ==
                          UnaryOperation::kIsNull) {
                    const Expression& child =
                        conjunct->AsUnaryExpression().Child();
                    if (child && child->Type() == TypeTag::kColumnValue) {
                      const ColumnName& col =
                          child->AsColumnValue().GetColumnName();
                      if (!col.schema.empty() && b_rels.contains(col.schema)) {
                        const bool not_null_in_match =
                            ColumnIsDeclaredNonNull(memo, col.schema, col) ||
                            (ojoin.predicate && *ojoin.predicate &&
                             ExpressionRejectsNullsWhere(
                                 *ojoin.predicate,
                                 [&](const ColumnName& c) {
                                   return c.schema == col.schema &&
                                          c.name == col.name;
                                 },
                                 /*bare_column_rejects_null=*/false));
                        if (not_null_in_match) {
                          ++null_checks;
                          continue;
                        }
                      }
                    }
                  }
                  if (ContainsAggregate(conjunct) ||
                      TreeContainsWindowCall(conjunct) ||
                      !SafeToReduceEvaluationCount(conjunct)) {
                    convertible = false;
                    break;
                  }
                  const auto touched = conjunct->TouchedColumns();
                  if (touched.empty() ||
                      !std::ranges::all_of(touched, [&](const ColumnName& c) {
                        return !c.schema.empty() &&
                               ac_rels.contains(c.schema) &&
                               !b_rels.contains(c.schema);
                      })) {
                    convertible = false;
                    break;
                  }
                  ac_residuals.push_back(conjunct);
                }
                if (!convertible || null_checks == 0) {
                  continue;
                }

                std::vector<std::string> ac_rels_vec = a_rels_vec;
                ac_rels_vec.reserve(a_rels_vec.size() + c_rels_vec.size());
                for (const std::string& r : c_rels_vec) {
                  if (std::ranges::find(ac_rels_vec, r) == ac_rels_vec.end()) {
                    ac_rels_vec.push_back(r);
                  }
                }
                const GroupId ac_join_group = memo.EnsureDerivedGroup(
                    ac_rels_vec,
                    "anti_thru_inner_ac:" + GroupIdsTag({a_id, c_id}) +
                        PredicateTag(join_c.predicate));
                InheritBranchLocal(memo, ac_join_group, group);
                if (ac_join_group == group || ac_join_group == input_id) {
                  continue;
                }
                memo.AddExpression(
                    ac_join_group,
                    LogicalExpression{.operation = LogicalOperator::kJoin,
                                      .children = {a_id, c_id},
                                      .predicate = join_c.predicate});

                if (ac_residuals.empty()) {
                  memo.AddExpression(
                      group,
                      LogicalExpression{.operation = LogicalOperator::kAntiJoin,
                                        .children = {ac_join_group, b_id},
                                        .predicate = ojoin.predicate,
                                        .target_list = sel_targets,
                                        .output_schema = sel_schema});
                } else {
                  // Distinct prefix from the plain outer_to_anti_join rule:
                  // the child pair here is (Join(A,C), B) instead of (L, R),
                  // so with the same prefix the two rules could collide on
                  // equal group ids and mix different AntiJoin meanings.
                  const GroupId anti_group = memo.EnsureDerivedGroup(
                      input_relations, "outer_to_anti_base_thru:" +
                                           GroupIdsTag({ac_join_group, b_id}) +
                                           PredicateTag(ojoin.predicate));
                  InheritBranchLocal(memo, anti_group, group);
                  if (anti_group == group || anti_group == input_id) {
                    continue;
                  }
                  memo.AddExpression(
                      anti_group,
                      LogicalExpression{.operation = LogicalOperator::kAntiJoin,
                                        .children = {ac_join_group, b_id},
                                        .predicate = ojoin.predicate});
                  memo.AddExpression(
                      group, LogicalExpression{
                                 .operation = LogicalOperator::kSelection,
                                 .children = {anti_group},
                                 .predicate = CanonicalizeConjuncts(
                                     CombineConjuncts(ac_residuals)),
                                 .target_list = sel_targets,
                                 .output_schema = sel_schema});
                }
              }
            }
          }
        },
        LogicalOperator::kSelection));

    // self_join_agg_to_window: Eliminate a self-join between a relation `s` and
    // a grouped aggregate `SELECT g, AGG(x) FROM s GROUP BY g` over the exact
    // same unfiltered base table by computing `AGG(x) OVER (PARTITION BY s.g)`
    // in a single pass and enforcing `s.g IS NOT NULL` (TPC-DS Q1/Q30/Q65/Q81).
    // Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
    //   (self_join_group_agg_to_window_sound,
    //    self_join_agg_window_null_key_bug)
    built.Add(Rule(
        "self_join_agg_to_window", Projection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kProjection ||
              expression.children.size() != 1 ||
              expression.target_list.empty()) {
            return;
          }
          const GroupId sel_id = bindings.at("input");
          if (sel_id == group ||
              memo.Get(sel_id).tag.starts_with("self_join_win_sel:")) {
            return;
          }
          const LogicalExpression& orig_proj = expression;
          const std::vector<LogicalExpression> sel_candidates =
              memo.Get(sel_id).expressions;

          const auto rebind_rel = [](auto& self, const Expression& expr,
                                     const std::string& from_rel,
                                     const std::string& to_rel) -> Expression {
            if (!expr) {
              return expr;
            }
            if (expr->Type() == TypeTag::kColumnValue) {
              ColumnName col = expr->AsColumnValue().GetColumnName();
              if (col.schema == from_rel) {
                col.schema = to_rel;
                return ColumnValueExp(col);
              }
              return expr;
            }
            std::vector<Expression> kids = ExpressionChildren(expr);
            bool changed = false;
            for (Expression& k : kids) {
              Expression rk = self(self, k, from_rel, to_rel);
              if (rk.get() != k.get()) {
                changed = true;
                k = std::move(rk);
              }
            }
            return changed ? WithExpressionChildren(expr, std::move(kids))
                           : expr;
          };

          const auto map_agg_refs =
              [](auto& self, const Expression& expr, const std::string& s_rel,
                 const std::string& b_rel, const std::string& key_name,
                 const std::string& agg_out_name,
                 const std::string& win_col_name) -> std::optional<Expression> {
            if (!expr) {
              return expr;
            }
            if (expr->Type() == TypeTag::kColumnValue) {
              const ColumnName& col = expr->AsColumnValue().GetColumnName();
              if (col.schema == s_rel) {
                return expr;
              }
              if ((col.schema.empty() || col.schema == b_rel) &&
                  col.name == agg_out_name) {
                return ColumnValueExp(ColumnName("", win_col_name));
              }
              if (col.schema == b_rel && col.name == key_name) {
                return ColumnValueExp(ColumnName(s_rel, key_name));
              }
              return std::nullopt;
            }
            if (expr->Type() == TypeTag::kQueryExp ||
                expr->Type() == TypeTag::kAggregateExp ||
                expr->Type() == TypeTag::kWindowFunctionExp) {
              return std::nullopt;
            }
            std::vector<Expression> kids = ExpressionChildren(expr);
            for (Expression& k : kids) {
              std::optional<Expression> mk = self(
                  self, k, s_rel, b_rel, key_name, agg_out_name, win_col_name);
              if (!mk.has_value()) {
                return std::nullopt;
              }
              k = std::move(*mk);
            }
            return WithExpressionChildren(expr, std::move(kids));
          };

          for (const LogicalExpression& sel_expr : sel_candidates) {
            if (sel_expr.operation != LogicalOperator::kSelection ||
                sel_expr.children.size() != 1 || !sel_expr.predicate ||
                !*sel_expr.predicate) {
              continue;
            }
            const GroupId join_id = sel_expr.children[0];
            const std::vector<LogicalExpression> join_candidates =
                memo.Get(join_id).expressions;
            for (const LogicalExpression& join_expr : join_candidates) {
              if (join_expr.operation != LogicalOperator::kJoin ||
                  join_expr.children.size() != 2) {
                continue;
              }
              for (int side = 0; side < 2; ++side) {
                const GroupId s_id = join_expr.children[side];
                const GroupId agg_id = join_expr.children[1 - side];
                if (memo.Get(s_id).relations.size() != 1 ||
                    memo.Get(s_id).filter || memo.Get(s_id).sample_barrier ||
                    memo.Get(agg_id).relations.size() != 1) {
                  continue;
                }
                const std::string s_rel = memo.Get(s_id).relations[0];
                const std::string b_rel = memo.Get(agg_id).relations[0];
                if (s_rel == b_rel) {
                  continue;
                }
                // The same-table proof comes from the catalog-published
                // schemas, never from alias spelling: IsSameTable's "_N"
                // stripping conflates distinct real tables such as `orders`
                // and `orders_1` (T224). Both aliases must resolve to the
                // same physical table; relations without catalog schemas
                // (CTEs, hand-built memos) cannot prove it and are skipped.
                const auto& rel_schemas = memo.GetTableSchemas();
                const auto s_schema = rel_schemas.find(s_rel);
                const auto b_schema = rel_schemas.find(b_rel);
                if (s_schema == rel_schemas.end() ||
                    b_schema == rel_schemas.end() ||
                    s_schema->second.Name() != b_schema->second.Name() ||
                    s_schema->second.Name().empty()) {
                  continue;
                }
                const std::string shared_table(s_schema->second.Name());
                // kScan.table spells either the relation alias or the
                // physical table name; AddExpression already proved each
                // scan covers its group's relation, so resolving the table
                // field through the catalog identifies a scan of the shared
                // table under either spelling.
                const auto scans_shared_table = [&](GroupId scan_group) {
                  for (const LogicalExpression& e :
                       memo.Get(scan_group).expressions) {
                    if (e.operation != LogicalOperator::kScan ||
                        e.table.empty()) {
                      continue;
                    }
                    if (e.table == shared_table) {
                      return true;
                    }
                    const auto owner = rel_schemas.find(e.table);
                    if (owner != rel_schemas.end() &&
                        owner->second.Name() == shared_table) {
                      return true;
                    }
                  }
                  return false;
                };
                if (!scans_shared_table(s_id)) {
                  continue;
                }
                const std::vector<LogicalExpression> agg_candidates =
                    memo.Get(agg_id).expressions;
                for (const LogicalExpression& agg_expr : agg_candidates) {
                  if (agg_expr.operation != LogicalOperator::kAggregation ||
                      agg_expr.children.size() != 1 ||
                      (agg_expr.predicate && *agg_expr.predicate) ||
                      agg_expr.grouping_sets.size() != 1) {
                    continue;
                  }
                  const Expression& gk = agg_expr.grouping_sets[0];
                  if (!gk || gk->Type() != TypeTag::kColumnValue) {
                    continue;
                  }
                  // AddExpression now retains PARTITION BY as metadata. When
                  // it agrees with the single grouping key the rewrite below
                  // rebuilds the same partitioning as a window, so the
                  // aggregate is still a valid candidate. A different
                  // partition list would change the rebuilt values and stays
                  // excluded.
                  if (!agg_expr.partition_by.empty() &&
                      (agg_expr.partition_by.size() != 1 ||
                       !agg_expr.partition_by[0] ||
                       ExpressionIdentity(agg_expr.partition_by[0]) !=
                           ExpressionIdentity(gk))) {
                    continue;
                  }
                  const ColumnName& gk_col =
                      gk->AsColumnValue().GetColumnName();
                  if (gk_col.schema != b_rel || gk_col.name.empty()) {
                    continue;
                  }
                  const GroupId b_scan_id = agg_expr.children[0];
                  if (memo.Get(b_scan_id).relations.size() != 1 ||
                      memo.Get(b_scan_id).filter ||
                      memo.Get(b_scan_id).sample_barrier) {
                    continue;
                  }
                  if (!scans_shared_table(b_scan_id)) {
                    continue;
                  }

                  std::string agg_out_name;
                  const AggregateExpression* single_agg = nullptr;
                  bool targets_valid = true;
                  for (const NamedExpression& t : agg_expr.target_list) {
                    if (!t.expression) {
                      targets_valid = false;
                      break;
                    }
                    if (t.expression->Type() == TypeTag::kColumnValue) {
                      const ColumnName& tc =
                          t.expression->AsColumnValue().GetColumnName();
                      if (tc.schema != b_rel || tc.name != gk_col.name) {
                        targets_valid = false;
                        break;
                      }
                      continue;
                    }
                    if (t.expression->Type() == TypeTag::kAggregateExp &&
                        single_agg == nullptr && !t.name.empty()) {
                      const auto& a = t.expression->AsAggregateExpression();
                      if (a.Distinct() || a.NeedsGroupContext() ||
                          a.HasWhereFilter() || !a.InnerGroupBy().empty() ||
                          !a.InnerOrderBy().empty()) {
                        targets_valid = false;
                        break;
                      }
                      if (a.GetType() != AggregationType::kSum &&
                          a.GetType() != AggregationType::kAvg &&
                          a.GetType() != AggregationType::kMin &&
                          a.GetType() != AggregationType::kMax &&
                          a.GetType() != AggregationType::kCount) {
                        targets_valid = false;
                        break;
                      }
                      if (!a.Child() ||
                          !SafeToReduceEvaluationCount(a.Child())) {
                        targets_valid = false;
                        break;
                      }
                      const auto a_cols = a.Child()->TouchedColumns();
                      if (!std::ranges::all_of(
                              a_cols, [&](const ColumnName& c) {
                                return c.name == "*" || c.schema == b_rel;
                              })) {
                        targets_valid = false;
                        break;
                      }
                      single_agg = &a;
                      agg_out_name = t.name;
                      continue;
                    }
                    targets_valid = false;
                    break;
                  }
                  if (!targets_valid || single_agg == nullptr) {
                    continue;
                  }

                  const std::vector<Expression> join_conjuncts =
                      join_expr.predicate && *join_expr.predicate
                          ? SplitConjuncts(*join_expr.predicate)
                          : std::vector<Expression>{};
                  const std::vector<Expression> sel_conjuncts =
                      SplitConjuncts(*sel_expr.predicate);
                  std::vector<Expression> all_conjuncts;
                  all_conjuncts.reserve(join_conjuncts.size() +
                                        sel_conjuncts.size());
                  all_conjuncts.insert(all_conjuncts.end(),
                                       join_conjuncts.begin(),
                                       join_conjuncts.end());
                  all_conjuncts.insert(all_conjuncts.end(),
                                       sel_conjuncts.begin(),
                                       sel_conjuncts.end());

                  const std::string win_col_name = "$self_win_" + agg_out_name;
                  bool found_key_eq = false;
                  bool conjuncts_ok = true;
                  std::vector<Expression> mapped_conjuncts;
                  mapped_conjuncts.reserve(all_conjuncts.size() + 1);
                  mapped_conjuncts.push_back(UnaryExpressionExp(
                      ColumnValueExp(ColumnName(s_rel, gk_col.name)),
                      UnaryOperation::kIsNotNull));
                  for (const Expression& c : all_conjuncts) {
                    if (!c || !SafeToReduceEvaluationCount(c)) {
                      conjuncts_ok = false;
                      break;
                    }
                    if (c->Type() == TypeTag::kBinaryExp &&
                        c->AsBinaryExpression().Op() ==
                            BinaryOperation::kEquals) {
                      const auto& bin = c->AsBinaryExpression();
                      if (bin.Left()->Type() == TypeTag::kColumnValue &&
                          bin.Right()->Type() == TypeTag::kColumnValue) {
                        const ColumnName& lc =
                            bin.Left()->AsColumnValue().GetColumnName();
                        const ColumnName& rc =
                            bin.Right()->AsColumnValue().GetColumnName();
                        const bool match_sb =
                            lc.schema == s_rel && lc.name == gk_col.name &&
                            (rc.schema == b_rel || rc.schema.empty()) &&
                            rc.name == gk_col.name;
                        const bool match_bs =
                            rc.schema == s_rel && rc.name == gk_col.name &&
                            (lc.schema == b_rel || lc.schema.empty()) &&
                            lc.name == gk_col.name;
                        if (match_sb || match_bs) {
                          found_key_eq = true;
                          continue;
                        }
                      }
                    }
                    std::optional<Expression> mc =
                        map_agg_refs(map_agg_refs, c, s_rel, b_rel, gk_col.name,
                                     agg_out_name, win_col_name);
                    if (!mc.has_value()) {
                      conjuncts_ok = false;
                      break;
                    }
                    mapped_conjuncts.push_back(std::move(*mc));
                  }
                  if (!found_key_eq || !conjuncts_ok) {
                    continue;
                  }

                  std::vector<NamedExpression> mapped_proj;
                  mapped_proj.reserve(orig_proj.target_list.size());
                  bool proj_ok = true;
                  for (const NamedExpression& pt : orig_proj.target_list) {
                    std::optional<Expression> mp =
                        map_agg_refs(map_agg_refs, pt.expression, s_rel, b_rel,
                                     gk_col.name, agg_out_name, win_col_name);
                    if (!mp.has_value()) {
                      proj_ok = false;
                      break;
                    }
                    mapped_proj.emplace_back(pt.name, std::move(*mp));
                  }
                  if (!proj_ok) {
                    continue;
                  }

                  std::string fn_name;
                  switch (single_agg->GetType()) {
                    case AggregationType::kSum:
                      fn_name = "SUM";
                      break;
                    case AggregationType::kAvg:
                      fn_name = "AVG";
                      break;
                    case AggregationType::kMin:
                      fn_name = "MIN";
                      break;
                    case AggregationType::kMax:
                      fn_name = "MAX";
                      break;
                    case AggregationType::kCount:
                      fn_name = "COUNT";
                      break;
                    default:
                      continue;
                  }
                  Expression rebound_arg =
                      rebind_rel(rebind_rel, single_agg->Child(), b_rel, s_rel);
                  Expression part_key =
                      ColumnValueExp(ColumnName(s_rel, gk_col.name));
                  Expression win_call = WindowFunctionCallExp(
                      std::move(fn_name), {std::move(rebound_arg)}, {part_key},
                      {});

                  // The rewritten chain must preserve the projection
                  // group's relation set {s_rel, b_rel}: a lone {s_rel}
                  // chain cannot be added to `group` (AddExpression rejects
                  // a unary child whose relation set differs). Like
                  // self_join_elimination, the shared base table is scanned
                  // once under a group covering both aliases; the catalog
                  // proof above is what lets that single scan stand for
                  // both relations.
                  const std::vector<std::string> out_rels =
                      memo.Get(group).relations;
                  const GroupId win_base = memo.EnsureDerivedGroup(
                      out_rels, "self_join_win_base:" +
                                    GroupIdsTag({s_id, agg_id}) + shared_table);
                  InheritBranchLocal(memo, win_base, group);
                  if (win_base == group || win_base == s_id ||
                      win_base == agg_id) {
                    continue;
                  }
                  memo.AddExpression(
                      win_base,
                      LogicalExpression{.operation = LogicalOperator::kScan,
                                        .table = shared_table});

                  const GroupId win_group = memo.EnsureDerivedGroup(
                      out_rels, "self_join_win:" + GroupIdsTag({win_base}) +
                                    win_col_name + "=" + win_call->ToString());
                  InheritBranchLocal(memo, win_group, group);
                  if (win_group == group || win_group == win_base) {
                    continue;
                  }
                  memo.AddExpression(
                      win_group,
                      LogicalExpression{.operation = LogicalOperator::kWindow,
                                        .children = {win_base},
                                        .target_list = {NamedExpression(
                                            win_col_name, std::move(win_call))},
                                        .partition_by = {std::move(part_key)}});

                  const Expression new_sel_pred =
                      CanonicalizeConjuncts(CombineConjuncts(mapped_conjuncts));
                  const GroupId new_sel_group = memo.EnsureDerivedGroup(
                      out_rels,
                      "self_join_win_sel:" + GroupIdsTag({win_group}) +
                          new_sel_pred->ToString());
                  InheritBranchLocal(memo, new_sel_group, group);
                  if (new_sel_group == group || new_sel_group == win_group ||
                      new_sel_group == win_base) {
                    continue;
                  }
                  memo.AddExpression(
                      new_sel_group,
                      LogicalExpression{
                          .operation = LogicalOperator::kSelection,
                          .children = {win_group},
                          .predicate = new_sel_pred});

                  LogicalExpression new_proj = orig_proj;
                  new_proj.children = {new_sel_group};
                  new_proj.target_list = std::move(mapped_proj);
                  memo.AddExpression(group, std::move(new_proj));
                }
              }
            }
          }
        },
        LogicalOperator::kProjection));

    // eager_distinct_fact_keys_before_join: Under
    // Distinct(Projection(Join(Join(fact, d1, p1), d2, p2))), when Projection
    // outputs zero columns of single-relation `fact` and `p1`, `p2` touch at
    // most 4 columns of `fact`, deduplicating `fact` onto those join surrogate
    // keys before joining `d1` and `d2` preserves the final set exactly while
    // shrinking multi-million-row fact inputs early (TPC-DS Q38, Q87).
    // Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
    //   (eager_distinct_surrogate_keys_before_dim_joins_sound)
    built.Add(Rule(
        "eager_distinct_fact_keys_before_join", Distinct(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kDistinct ||
              expression.children.size() != 1) {
            return;
          }
          const GroupId proj_id = bindings.at("input");
          if (proj_id == group) {
            return;
          }
          const LogicalExpression& orig_dist = expression;
          const std::vector<LogicalExpression> proj_candidates =
              memo.Get(proj_id).expressions;
          const std::vector<std::string> proj_rels_vec =
              memo.Get(proj_id).relations;

          for (const LogicalExpression& proj_expr : proj_candidates) {
            if (proj_expr.operation != LogicalOperator::kProjection ||
                proj_expr.children.size() != 1 ||
                proj_expr.target_list.empty()) {
              continue;
            }
            const GroupId j2_id = proj_expr.children[0];
            if (memo.Get(j2_id).tag.starts_with("eager_distinct_post_j2:")) {
              continue;
            }
            std::unordered_set<std::string> proj_rels;
            bool proj_cols_ok = true;
            for (const NamedExpression& t : proj_expr.target_list) {
              if (!t.expression || ContainsAggregate(t.expression) ||
                  TreeContainsWindowCall(t.expression) ||
                  !SafeToReduceEvaluationCount(t.expression)) {
                proj_cols_ok = false;
                break;
              }
              for (const ColumnName& c : t.expression->TouchedColumns()) {
                if (c.schema.empty() || c.name == "*") {
                  proj_cols_ok = false;
                  break;
                }
                proj_rels.insert(c.schema);
              }
              if (!proj_cols_ok) {
                break;
              }
            }
            if (!proj_cols_ok || proj_rels.empty()) {
              continue;
            }

            const std::vector<LogicalExpression> j2_candidates =
                memo.Get(j2_id).expressions;
            const std::vector<std::string> j2_rels = memo.Get(j2_id).relations;
            for (const LogicalExpression& j2 : j2_candidates) {
              if (j2.operation != LogicalOperator::kJoin ||
                  j2.children.size() != 2 || !j2.predicate || !*j2.predicate ||
                  !SafeToReduceEvaluationCount(*j2.predicate)) {
                continue;
              }
              for (int j2_side = 0; j2_side < 2; ++j2_side) {
                const GroupId j1_id = j2.children[j2_side];
                const GroupId d2_id = j2.children[1 - j2_side];
                const std::vector<LogicalExpression> j1_candidates =
                    memo.Get(j1_id).expressions;
                const std::vector<std::string> j1_rels =
                    memo.Get(j1_id).relations;
                const std::vector<std::string> d2_rels_vec =
                    memo.Get(d2_id).relations;
                const std::unordered_set<std::string> d2_rels(
                    d2_rels_vec.begin(), d2_rels_vec.end());

                for (const LogicalExpression& j1 : j1_candidates) {
                  if (j1.operation != LogicalOperator::kJoin ||
                      j1.children.size() != 2 || !j1.predicate ||
                      !*j1.predicate ||
                      !SafeToReduceEvaluationCount(*j1.predicate)) {
                    continue;
                  }
                  for (int j1_side = 0; j1_side < 2; ++j1_side) {
                    const GroupId fact_id = j1.children[j1_side];
                    const GroupId d1_id = j1.children[1 - j1_side];
                    if (memo.Get(fact_id).relations.size() != 1 ||
                        memo.Get(fact_id).tag.starts_with(
                            "eager_distinct_fact:")) {
                      continue;
                    }
                    const std::string fact_rel = memo.Get(fact_id).relations[0];
                    if (proj_rels.contains(fact_rel)) {
                      continue;
                    }
                    const std::vector<std::string> d1_rels_vec =
                        memo.Get(d1_id).relations;
                    const std::unordered_set<std::string> d1_rels(
                        d1_rels_vec.begin(), d1_rels_vec.end());
                    if (d1_rels.contains(fact_rel) ||
                        d2_rels.contains(fact_rel)) {
                      continue;
                    }

                    std::vector<ColumnName> fact_keys;
                    // fact_keys.size() > 4 is rejected below, so the accepted
                    // path needs at most that many entries; reserve to avoid
                    // reallocating while collecting them from both predicates.
                    fact_keys.reserve(8);
                    bool preds_ok = true;
                    for (const Expression* pred :
                         {&(*j1.predicate), &(*j2.predicate)}) {
                      if (ContainsAggregate(*pred) ||
                          TreeContainsWindowCall(*pred)) {
                        preds_ok = false;
                        break;
                      }
                      for (const ColumnName& c : (*pred)->TouchedColumns()) {
                        if (c.schema.empty() || c.name == "*") {
                          preds_ok = false;
                          break;
                        }
                        if (c.schema == fact_rel &&
                            std::ranges::find(fact_keys, c) ==
                                fact_keys.end()) {
                          fact_keys.push_back(c);
                        }
                      }
                    }
                    if (!preds_ok || fact_keys.empty() ||
                        fact_keys.size() > 4) {
                      continue;
                    }
                    std::ranges::sort(fact_keys, [](const ColumnName& a,
                                                    const ColumnName& b) {
                      return a.name < b.name;
                    });

                    std::vector<NamedExpression> fact_proj_targets;
                    fact_proj_targets.reserve(fact_keys.size());
                    std::string fk_tag;
                    for (const ColumnName& fk : fact_keys) {
                      fact_proj_targets.emplace_back(fk);
                      fk_tag += fk.name + ",";
                    }
                    const std::vector<std::string> fact_rels_vec{fact_rel};
                    const GroupId fact_proj_group = memo.EnsureDerivedGroup(
                        fact_rels_vec, "eager_distinct_fact_proj:" +
                                           GroupIdsTag({fact_id}) + fk_tag);
                    InheritBranchLocal(memo, fact_proj_group, group);
                    if (fact_proj_group == group ||
                        fact_proj_group == fact_id) {
                      continue;
                    }
                    memo.AddExpression(
                        fact_proj_group,
                        LogicalExpression{
                            .operation = LogicalOperator::kProjection,
                            .children = {fact_id},
                            .target_list = std::move(fact_proj_targets)});

                    const GroupId fact_dist_group = memo.EnsureDerivedGroup(
                        fact_rels_vec, "eager_distinct_fact:" +
                                           GroupIdsTag({fact_proj_group}) +
                                           fk_tag);
                    InheritBranchLocal(memo, fact_dist_group, group);
                    if (fact_dist_group == group ||
                        fact_dist_group == fact_proj_group) {
                      continue;
                    }
                    memo.AddExpression(
                        fact_dist_group,
                        LogicalExpression{
                            .operation = LogicalOperator::kDistinct,
                            .children = {fact_proj_group}});

                    const GroupId new_j1_left =
                        j1_side == 0 ? fact_dist_group : d1_id;
                    const GroupId new_j1_right =
                        j1_side == 0 ? d1_id : fact_dist_group;
                    const GroupId new_j1_group = memo.EnsureDerivedGroup(
                        j1_rels, "eager_distinct_post_j1:" +
                                     GroupIdsTag({new_j1_left, new_j1_right}) +
                                     PredicateTag(j1.predicate));
                    InheritBranchLocal(memo, new_j1_group, group);
                    if (new_j1_group == group || new_j1_group == j1_id) {
                      continue;
                    }
                    memo.AddExpression(
                        new_j1_group,
                        LogicalExpression{
                            .operation = LogicalOperator::kJoin,
                            .children = {new_j1_left, new_j1_right},
                            .predicate = j1.predicate});

                    const GroupId new_j2_left =
                        j2_side == 0 ? new_j1_group : d2_id;
                    const GroupId new_j2_right =
                        j2_side == 0 ? d2_id : new_j1_group;
                    const GroupId new_j2_group = memo.EnsureDerivedGroup(
                        j2_rels, "eager_distinct_post_j2:" +
                                     GroupIdsTag({new_j2_left, new_j2_right}) +
                                     PredicateTag(j2.predicate));
                    InheritBranchLocal(memo, new_j2_group, group);
                    if (new_j2_group == group || new_j2_group == j2_id) {
                      continue;
                    }
                    memo.AddExpression(
                        new_j2_group,
                        LogicalExpression{
                            .operation = LogicalOperator::kJoin,
                            .children = {new_j2_left, new_j2_right},
                            .predicate = j2.predicate});

                    LogicalExpression new_proj = proj_expr;
                    new_proj.children = {new_j2_group};
                    const GroupId new_proj_group = memo.EnsureDerivedGroup(
                        proj_rels_vec,
                        "eager_distinct_post_proj:" +
                            GroupIdsTag({new_j2_group}) +
                            TargetListFingerprint(new_proj.target_list));
                    InheritBranchLocal(memo, new_proj_group, group);
                    if (new_proj_group == group || new_proj_group == proj_id) {
                      continue;
                    }
                    memo.AddExpression(new_proj_group, std::move(new_proj));

                    LogicalExpression new_dist = orig_dist;
                    new_dist.children = {new_proj_group};
                    memo.AddExpression(group, std::move(new_dist));
                  }
                }
              }
            }
          }
        },
        LogicalOperator::kDistinct));

    return built;
  }();
  return rules;
}

}  // namespace tinylamb::cascades
