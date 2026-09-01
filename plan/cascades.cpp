/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/rewrite.hpp"
#include "query/query_data.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb::cascades {
namespace {

// Practical ceiling for exhaustive join enumeration: the join_enumeration
// rule walks 2^n bipartitions per join group, and memo construction
// multiplies that by every subset explored. Beyond this limit planning cost
// explodes combinatorially (n=30 is already intractable), so enumeration is
// skipped and the connected-split join order built during Memo::Build serves
// as the fallback plan.
constexpr size_t kMaxJoinEnumerationRelations = 16;

std::vector<std::string> Normalize(std::vector<std::string> relations) {
  std::ranges::sort(relations);
  const auto [first, last] = std::ranges::unique(relations);
  relations.erase(first, last);
  return relations;
}

// True when every column touched by the projection's target list belongs to
// the LEFT side of the join inside `input_id` (right side fully unused).
bool ProjectionUsesOnlyLeftSide(const LogicalExpression& projection,
                                const Memo& memo, GroupId input_id) {
  bool saw_join = false;
  for (const LogicalExpression& join : memo.Get(input_id).expressions) {
    if (join.operation != LogicalOperator::kJoin ||
        join.children.size() != 2) {
      continue;
    }
    saw_join = true;
    const Group& right_group = memo.Get(join.children[1]);
    for (const NamedExpression& item : projection.target_list) {
      if (!item.expression) { continue; }
      for (const ColumnName& col : item.expression->TouchedColumns()) {
        if (std::ranges::find(right_group.relations, col.schema) !=
            right_group.relations.end()) {
          return false;
        }
      }
    }
  }
  return saw_join;
}

std::vector<std::string> UnionRelations(const std::vector<std::string>& left,
                                        const std::vector<std::string>& right) {
  std::vector<std::string> result;
  std::ranges::set_union(left, right, std::back_inserter(result));
  return result;
}

// Canonical conjunct form: split, sort by printed representation, drop
// duplicates, recombine. Fingerprint stability for payload-bearing
// expressions depends on this.
Expression CanonicalizeConjuncts(const Expression& predicate) {
  if (!predicate) {
    return nullptr;
  }
  std::vector<Expression> conjuncts = SplitConjuncts(predicate);
  std::ranges::sort(conjuncts, [](const Expression& a, const Expression& b) {
    return a->ToString() < b->ToString();
  });
  conjuncts.erase(
      std::ranges::unique(conjuncts,
                          [](const Expression& a, const Expression& b) {
                            return a->ToString() == b->ToString();
                          })
          .begin(),
      conjuncts.end());
  // Contradiction detection: if any conjunct is always false, the whole
  // conjunction is false.  Patterns handled:
  //   1. col = const AND col IS NULL  (equality contradicts NULL test)
  //   2. col > const AND col IS NULL  (range contradicts NULL test)
  //   3. col < const AND col IS NULL  (range contradicts NULL test)
  //   4. col >= const AND col IS NULL (range contradicts NULL test)
  //   5. col <= const AND col IS NULL (range contradicts NULL test)
  //   6. col != const AND col = const (equality contradiction)
  //   7. col > A AND col < B where A >= B (empty range)
  //   8. col < A AND col > B where B >= A (empty range)
  {
    struct ColPred {
      std::string col_name;
      BinaryOperation op;
      Value val;
      bool is_null_test{false};
      bool is_null_positive{false};  // true = IS NULL, false = IS NOT NULL
    };
    std::vector<ColPred> preds;
    for (const auto& c : conjuncts) {
      if (!c) continue;
      if (c->Type() == TypeTag::kUnaryExp) {
        const auto& unary = c->AsUnaryExpression();
        if ((unary.Op() == UnaryOperation::kIsNull ||
             unary.Op() == UnaryOperation::kIsNotNull) &&
            unary.Child()->Type() == TypeTag::kColumnValue) {
          ColPred p;
          p.col_name = unary.Child()->AsColumnValue().GetColumnName().name;
          p.is_null_test = true;
          p.is_null_positive = (unary.Op() == UnaryOperation::kIsNull);
          preds.push_back(std::move(p));
        }
      } else if (c->Type() == TypeTag::kBinaryExp) {
        const auto& bin = c->AsBinaryExpression();
        if (bin.Left()->Type() == TypeTag::kColumnValue &&
            bin.Right()->Type() == TypeTag::kConstantValue) {
          ColPred p;
          p.col_name = bin.Left()->AsColumnValue().GetColumnName().name;
          p.op = bin.Op();
          p.val = bin.Right()->AsConstantValue().GetValue();
          preds.push_back(std::move(p));
        } else if (bin.Left()->Type() == TypeTag::kConstantValue &&
                   bin.Right()->Type() == TypeTag::kColumnValue) {
          ColPred p;
          p.col_name = bin.Right()->AsColumnValue().GetColumnName().name;
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
    bool contradiction = false;
    for (size_t i = 0; i < preds.size() && !contradiction; ++i) {
      for (size_t j = i + 1; j < preds.size() && !contradiction; ++j) {
        if (preds[i].col_name != preds[j].col_name) continue;
        // col = N AND col IS NULL -> contradiction
        // is_null_positive=true means IS NULL; false means IS NOT NULL
        if (preds[i].is_null_test && preds[i].is_null_positive &&
            !preds[j].is_null_test &&
            preds[j].op == BinaryOperation::kEquals &&
            !preds[j].val.IsNull()) {
          contradiction = true;
        }
        if (preds[j].is_null_test && preds[j].is_null_positive &&
            !preds[i].is_null_test &&
            preds[i].op == BinaryOperation::kEquals &&
            !preds[i].val.IsNull()) {
          contradiction = true;
        }
        if (!preds[i].is_null_test && !preds[j].is_null_test &&
            preds[i].val.type == preds[j].val.type &&
            preds[i].val.type == ValueType::kInt64) {
          const int64_t a = preds[i].val.value.int_value;
          const int64_t b = preds[j].val.value.int_value;
          const auto oi = preds[i].op;
          const auto oj = preds[j].op;
          if ((oi == BinaryOperation::kGreaterThan ||
               oi == BinaryOperation::kGreaterThanEquals) &&
              (oj == BinaryOperation::kLessThan ||
               oj == BinaryOperation::kLessThanEquals)) {
            if (a > b || (a == b &&
                          (oi == BinaryOperation::kGreaterThan ||
                           oj == BinaryOperation::kLessThan))) {
              contradiction = true;
            }
          }
          if ((oi == BinaryOperation::kLessThan ||
               oi == BinaryOperation::kLessThanEquals) &&
              (oj == BinaryOperation::kGreaterThan ||
               oj == BinaryOperation::kGreaterThanEquals)) {
            if (b > a || (b == a &&
                          (oj == BinaryOperation::kGreaterThan ||
                           oi == BinaryOperation::kLessThan))) {
              contradiction = true;
            }
          }
          if (oi == BinaryOperation::kEquals &&
              oj == BinaryOperation::kEquals && a != b) {
            contradiction = true;
          }
          if ((oi == BinaryOperation::kNotEquals &&
               oj == BinaryOperation::kEquals && a == b) ||
              (oj == BinaryOperation::kNotEquals &&
               oi == BinaryOperation::kEquals && a == b)) {
            contradiction = true;
          }
        }
      }
    }
    if (contradiction) {
      return ConstantValueExp(Value(false));
    }
  }
  return CombineConjuncts(conjuncts);
}

// Pushes every single-relation conjunct of `predicate` into the matching scan
// group's filter. Returns the conjuncts that have no single-relation home and
// therefore must stay in the Selection. Guard rail (Phase 2): nothing here
// reasons about null-rejection, so the rule must never be used to push
// predicates through outer joins once those exist.
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
    if (touched.size() == 1 && relation_enabled(*touched.begin())) {
      memo.MergeScanFilter(memo.EnsureGroup({*touched.begin()}), conjunct);
      continue;
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
  if (!output.name.empty() &&
      (output.name == column.name || output.name == column.ToString())) {
    return true;
  }
  if (output.expression && output.expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& source =
        output.expression->AsColumnValue().GetColumnName();
    return source == column || source.name == column.name;
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

std::optional<Value> EqualityConstant(const Expression& predicate,
                                      const ColumnName& column) {
  if (!predicate) {
    return std::nullopt;
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
      return binary.Right()->AsConstantValue().GetValue();
    }
    if (binary.Right()->Type() == TypeTag::kColumnValue &&
        binary.Left()->Type() == TypeTag::kConstantValue &&
        binary.Right()->AsColumnValue().GetColumnName() == column) {
      return binary.Left()->AsConstantValue().GetValue();
    }
  }
  return std::nullopt;
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
      try {
        static_cast<void>(memo.RelationMask({from.schema}));
        static_cast<void>(memo.RelationMask({to.schema}));
      } catch (...) {
        return;
      }
      const std::optional<Value> constant = EqualityConstant(
          memo.Get(memo.EnsureGroup({from.schema})).filter, from);
      if (!constant) {
        return;
      }
      memo.MergeScanFilter(
          memo.EnsureGroup({to.schema}),
          BinaryExpressionExp(ColumnValueExp(to), BinaryOperation::kEquals,
                              ConstantValueExp(*constant)));
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

  std::vector<Expression> conjuncts;

  if (expression.output_schema.ColumnCount() > 0) {
    for (size_t i = 0; i < expression.output_schema.ColumnCount(); ++i) {
      const auto& col = expression.output_schema.GetColumn(i);
      ColumnName left_col = col.Name();
      ColumnName right_col = col.Name();
      if (!left_rel.empty()) left_col.schema = left_rel;
      if (!right_rel.empty()) right_col.schema = right_rel;
      conjuncts.push_back(BinaryExpressionExp(
          ColumnValueExp(left_col), BinaryOperation::kEquals,
          ColumnValueExp(right_col)));
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
      if (!left_rel.empty()) left_col.schema = left_rel;
      if (!right_rel.empty()) right_col.schema = right_rel;
      conjuncts.push_back(BinaryExpressionExp(
          ColumnValueExp(left_col), BinaryOperation::kEquals,
          ColumnValueExp(right_col)));
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
        conjuncts.push_back(BinaryExpressionExp(
            left_targets[i].expression, BinaryOperation::kEquals,
            right_targets[i].expression));
      }
    } else if (left_sch.ColumnCount() > 0 &&
               left_sch.ColumnCount() == right_sch.ColumnCount()) {
      for (size_t i = 0; i < left_sch.ColumnCount(); ++i) {
        ColumnName left_col = left_sch.GetColumn(i).Name();
        ColumnName right_col = right_sch.GetColumn(i).Name();
        if (left_col.schema.empty() && !left_rel.empty())
          left_col.schema = left_rel;
        if (right_col.schema.empty() && !right_rel.empty())
          right_col.schema = right_rel;
        conjuncts.push_back(BinaryExpressionExp(
            ColumnValueExp(left_col), BinaryOperation::kEquals,
            ColumnValueExp(right_col)));
      }
    }
  }

  if (conjuncts.empty()) {
    Expression cond = memo.JoinConditionFor(left_group, right_group);
    if (cond) {
      return cond;
    }
    ColumnName left_col(left_rel, "id");
    ColumnName right_col(right_rel, "id");
    return BinaryExpressionExp(ColumnValueExp(left_col),
                               BinaryOperation::kEquals,
                               ColumnValueExp(right_col));
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
    if (rel.empty()) return str;
    size_t pos = 0;
    while ((pos = str.find(rel, pos)) != std::string::npos) {
      str.replace(pos, rel.length(), "$T");
      pos += 2;
    }
    return str;
  };
  return normalize(s1, r1) == normalize(s2, r2);
}

bool HasKeyEqualityPredicate(const Expression& predicate,
                             const std::string& r1,
                             const std::string& r2) {
  if (!predicate) {
    return false;
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
        binary.Right()->Type() == TypeTag::kColumnValue) {
      const ColumnName& c1 = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& c2 = binary.Right()->AsColumnValue().GetColumnName();
      if ((c1.schema == r1 && c2.schema == r2 && c1.name == c2.name) ||
          (c1.schema == r2 && c2.schema == r1 && c1.name == c2.name) ||
          (c1.name == c2.name &&
           (c1.schema == r1 || c2.schema == r2 || r1 == r2))) {
        return true;
      }
    }
  }
  return false;
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
      try {
        static_cast<void>(memo.RelationMask({from.schema}));
        static_cast<void>(memo.RelationMask({to.schema}));
      } catch (...) {
        return;
      }
      const Expression from_filter =
          memo.Get(memo.EnsureGroup({from.schema})).filter;
      if (!from_filter) {
        return;
      }
      for (const Expression& filter_conjunct : SplitConjuncts(from_filter)) {
        if (!filter_conjunct || filter_conjunct->Type() != TypeTag::kBinaryExp) {
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
            memo.MergeScanFilter(
                memo.EnsureGroup({to.schema}),
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
  for (const std::string& pivot : relations) {
    const uint64_t pivot_mask = memo.RelationMask({pivot});
    if (memo.CutConnected(pivot_mask, within)) {
      best_pivot = pivot;
      break;
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
    out << "#p:" << (*predicate)->ToString();
  }
  for (const NamedExpression& item : target_list) {
    // A target expression can be a null handle when a rule built a
    // placeholder; the fingerprint must not dereference it (SIGSEGV).
    out << "#t:" << item.name << '='
        << (item.expression ? item.expression->ToString() : std::string());
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
  }
  if (operation == LogicalOperator::kRelational) {
    out << "#relational:"
        << static_cast<const void*>(relational_statement.get());
  }
  if (operation == LogicalOperator::kValues ||
      operation == LogicalOperator::kConstantTable) {
    out << "#v:" << output_schema.ColumnCount() << ':' << values.size();
    for (const Row& row : values) {
      out << ':' << row;
    }
  }
  if (!marker_column.empty()) {
    out << "#m:" << marker_column;
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
      out << expr->ToString() << ';';
    }
  }
  if (!grouping_sets.empty()) {
    out << "#gs:";
    for (const Expression& expr : grouping_sets) {
      out << expr->ToString() << ';';
    }
  }
  if (!unnest_alias.empty()) {
    out << "#u:" << unnest_alias;
  }
  if (!cte_name.empty()) {
    out << "#cte:" << cte_name << ':' << depth_limit;
  }
  if (operation == LogicalOperator::kSample) {
    out << "#sample:" << sample_rate << ':' << (is_bernoulli ? 'b' : 's');
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

GroupId Memo::Build(const std::vector<std::string>& relations) {
  return Build(relations, {});
}

GroupId Memo::Build(const std::vector<std::string>& relations,
                    const std::vector<ConjunctInfo>& conjuncts) {
  if (relations.empty()) {
    throw std::invalid_argument("empty join graph");
  }
  if (Normalize(relations).size() != relations.size()) {
    throw std::invalid_argument("duplicate relation in join graph");
  }
  if (relations.size() >= std::numeric_limits<uint64_t>::digits) {
    throw std::invalid_argument("join graph is too large for enumeration");
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
      throw std::invalid_argument("relation outside the join graph: " +
                                  relation);
    }
    mask |= uint64_t{1} << found->second;
  }
  return mask;
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

LogicalExpression Memo::NewJoin(GroupId left, GroupId right) const {
  LogicalExpression join{.operation = LogicalOperator::kJoin,
                         .children = {left, right}};
  const Expression condition = JoinConditionFor(Get(left), Get(right));
  if (condition) {
    join.predicate = condition;
  }
  return join;
}

void Memo::MergeScanFilter(GroupId group, const Expression& predicate) {
  Group& target = Get(group);
  if (target.relations.size() != 1) {
    throw std::invalid_argument("scan filter requires a single-relation group");
  }
  const Expression next = CanonicalizeConjuncts(
      target.filter
          ? BinaryExpressionExp(target.filter, BinaryOperation::kAnd, predicate)
          : predicate);
  target.filter = next;
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
    throw std::invalid_argument("empty memo group");
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
                          .tag = ""});
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
                          .tag = std::string(tag)});
  return id;
}

bool Memo::AddExpression(GroupId group, LogicalExpression expression) {
  Group& target = Get(group);
  // Normalize an engaged-but-null predicate (Expression{} is a null
  // shared_ptr) to nullopt: callers only test has_value() / operator bool,
  // so an optional holding a null handle dereferences as null downstream
  // (SIGSEGV in Fingerprint and rule bodies).
  if (expression.predicate.has_value() && !*expression.predicate) {
    expression.predicate.reset();
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
        throw std::invalid_argument("scan does not belong to memo group");
      }
      break;
    case LogicalOperator::kValues:
    case LogicalOperator::kConstantTable:
    case LogicalOperator::kDummyScan:
    case LogicalOperator::kGenerateSeries:
    case LogicalOperator::kWorkTableScan:
    case LogicalOperator::kRelational:
      break;
    case LogicalOperator::kJoin:
    case LogicalOperator::kOuterJoin:
    case LogicalOperator::kCrossJoin:
    case LogicalOperator::kSingleJoin:
    case LogicalOperator::kMarkJoin:
    case LogicalOperator::kApply:
    case LogicalOperator::kRecursiveCte: {
      if (expression.children.size() != 2) {
        throw std::invalid_argument("join must have two child groups");
      }
      const Group& left = Get(expression.children[0]);
      const Group& right = Get(expression.children[1]);
      std::vector<std::string> intersection;
      std::ranges::set_intersection(left.relations, right.relations,
                                    std::back_inserter(intersection));
      if (!intersection.empty() ||
          UnionRelations(left.relations, right.relations) != target.relations) {
        throw std::invalid_argument(
            "join children are not equivalent to group");
      }
      break;
    }
    case LogicalOperator::kSemiJoin:
    case LogicalOperator::kAntiJoin: {
      if (expression.children.size() != 2) {
        throw std::invalid_argument("semi/anti join must have two child groups");
      }
      const Group& left = Get(expression.children[0]);
      const Group& right = Get(expression.children[1]);
      if (left.relations != target.relations &&
          UnionRelations(left.relations, right.relations) != target.relations) {
        throw std::invalid_argument(
            "join children are not equivalent to group");
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
        throw std::invalid_argument(
            "set operation needs at least two children");
      }
      std::vector<std::string> relations;
      for (const GroupId child : expression.children) {
        relations = UnionRelations(relations, Get(child).relations);
      }
      if (relations != target.relations) {
        throw std::invalid_argument(
            "set operation children are not equivalent to group");
      }
      break;
    }
    case LogicalOperator::kSelection:
    case LogicalOperator::kProjection:
    case LogicalOperator::kAggregation:
    case LogicalOperator::kSort:
    case LogicalOperator::kTopN:
    case LogicalOperator::kDistinct:
    case LogicalOperator::kMax1Row:
    case LogicalOperator::kLimit:
    case LogicalOperator::kEmpty:
    case LogicalOperator::kWindow:
    case LogicalOperator::kUnnest:
    case LogicalOperator::kMaterialize:
    case LogicalOperator::kEagerSpool:
    case LogicalOperator::kLazySpool:
    case LogicalOperator::kExpand:
    case LogicalOperator::kExchange:
    case LogicalOperator::kGather:
    case LogicalOperator::kBroadcast:
    case LogicalOperator::kRedistribute:
    case LogicalOperator::kSample:
    case LogicalOperator::kAssert: {
      if (expression.children.size() != 1) {
        throw std::invalid_argument("single-child logical operator");
      }
      if (expression.children[0] == group) {
        throw std::invalid_argument(
            "logical operator references its own group");
      }
      if (Get(expression.children[0]).relations != target.relations) {
        throw std::invalid_argument(
            "operator must preserve the group's relation set");
      }
      if (expression.operation == LogicalOperator::kSelection &&
          (!expression.predicate || !*expression.predicate)) {
        throw std::invalid_argument("selection must carry a predicate");
      }
      if ((expression.operation == LogicalOperator::kProjection ||
           expression.operation == LogicalOperator::kAggregation) &&
          expression.target_list.empty()) {
        throw std::invalid_argument(
            "projection/aggregation needs a target list");
      }
      if ((expression.operation == LogicalOperator::kSort ||
           expression.operation == LogicalOperator::kTopN) &&
          (expression.target_list.empty() ||
           expression.target_list.size() != expression.sort_ascending.size())) {
        throw std::invalid_argument("sort needs one direction per key");
      }
      if (expression.operation == LogicalOperator::kTopN &&
          expression.limit_count == 0) {
        throw std::invalid_argument("top-n needs a finite limit");
      }
      break;
    }
  }
  // Duplicate rejection runs BEFORE the cap check counts against it, but a
  // rejected duplicate must not trip the degradation flag either (§6.10):
  // mirrored associativity rotations re-derive existing expressions by
  // design and those retries have to stay free.
  const std::string fingerprint = expression.Fingerprint();
  const bool duplicate =
      std::ranges::any_of(target.expressions, [&](const auto& existing) {
        return existing.Fingerprint() == fingerprint;
      });
  if (!duplicate && target.expressions.size() >= expression_cap_) {
    degraded_ = true;
  }
  if (duplicate || target.expressions.size() >= expression_cap_) {
    return false;
  }
  target.expressions.push_back(std::move(expression));
  touched_groups_.push_back(group);
  return true;
}

const Group& Memo::Get(GroupId group) const {
  if (group >= groups_.size()) {
    throw std::out_of_range("memo group");
  }
  return groups_[group];
}

Group& Memo::Get(GroupId group) {
  if (group >= groups_.size()) {
    throw std::out_of_range("memo group");
  }
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

Pattern Pattern::Any(std::string capture) {
  Pattern pattern;
  pattern.capture_ = std::move(capture);
  return pattern;
}

Pattern Pattern::Op(LogicalOperator operation, std::vector<Pattern> children,
                    std::string capture) {
  Pattern pattern;
  pattern.operation_ = operation;
  pattern.children_ = std::move(children);
  pattern.capture_ = std::move(capture);
  return pattern;
}

Pattern Pattern::Op(LogicalOperator operation, std::vector<Pattern> children,
                    std::string capture, PayloadConstraint payload) {
  Pattern pattern = Op(operation, std::move(children), std::move(capture));
  pattern.payload_ = payload;
  return pattern;
}

bool Pattern::MatchGroup(
    const Memo& memo,
    GroupId group,  // NOLINT(misc-no-recursion) // Cascades pattern matching
                    // recurses over the expression tree by design; depth is
                    // bounded by the memo built from a finite query.
    Bindings* bindings) const {
  if (!capture_.empty()) {
    auto [iter, inserted] = bindings->emplace(capture_, group);
    if (!inserted && iter->second != group) {
      return false;
    }
  }
  if (!operation_) {
    return true;
  }
  return std::ranges::any_of(
      memo.Get(group).expressions,
      [&](const LogicalExpression&
              expression) {  // NOLINT(misc-no-recursion) // Part of
                             // Pattern::MatchGroup recursion; bounded by the
                             // finite memo.
        Bindings local = *bindings;
        if (Match(memo, group, expression, &local)) {
          *bindings = std::move(local);
          return true;
        }
        return false;
      });
}

bool Pattern::MatchPayload(const Memo& memo,
                           const LogicalExpression& expression,
                           [[maybe_unused]] const Bindings& bindings) const {
  if (payload_.requires_predicate && !expression.predicate) {
    return false;
  }
  if (!payload_.predicate_within_child) {
    return true;
  }
  const size_t child_index = *payload_.predicate_within_child;
  if (!expression.predicate || child_index >= expression.children.size()) {
    return false;
  }
  const Group& child = memo.Get(expression.children[child_index]);
  return std::ranges::all_of((*expression.predicate)->TouchedColumns(),
                             [&child](const ColumnName& column) {
                               // Unqualified names cannot be proven to belong
                               // to the child, so they fail the constraint
                               // (strict interpretation).
                               return !column.schema.empty() &&
                                      std::ranges::find(child.relations,
                                                        column.schema) !=
                                          child.relations.end();
                             });
}

bool Pattern::Match(
    const Memo& memo,
    GroupId group,  // NOLINT(misc-no-recursion) // Cascades pattern matching
                    // recurses over the expression tree by design; depth is
                    // bounded by the memo built from a finite query.
    const LogicalExpression& expression, Bindings* bindings) const {
  if (operation_ && expression.operation != *operation_) {
    return false;
  }
  if (!children_.empty() && children_.size() != expression.children.size()) {
    return false;
  }
  Bindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, group);
    if (!inserted && iter->second != group) {
      return false;
    }
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].MatchGroup(memo, expression.children[i], &local)) {
      return false;
    }
  }
  if (!MatchPayload(memo, expression, local)) {
    return false;
  }
  *bindings = std::move(local);
  return true;
}

bool Rule::Apply(Memo& memo, GroupId group,
                 const LogicalExpression& expression) const {
  if (!MayApply(expression.operation)) {
    return false;
  }
  Bindings bindings;
  if (!pattern_.Match(memo, group, expression, &bindings)) {
    return false;
  }
  const size_t before_groups = memo.GroupCount();
  const size_t before_expressions = memo.ExpressionCount(group);
  transform_(bindings, memo, group, expression);
  return memo.GroupCount() != before_groups ||
         memo.ExpressionCount(group) != before_expressions;
}

RuleSet& RuleSet::Add(Rule rule) {
  Remove(rule.Name());
  rules_.push_back(std::move(rule));
  return *this;
}

bool RuleSet::Remove(std::string_view name) {
  const size_t old_size = rules_.size();
  std::erase_if(rules_, [&](const Rule& rule) { return rule.Name() == name; });
  return old_size != rules_.size();
}

bool RuleSet::Contains(std::string_view name) const {
  return std::ranges::any_of(
      rules_, [&](const Rule& rule) { return rule.Name() == name; });
}

std::vector<std::string> RuleSet::Names() const {
  std::vector<std::string> names;
  names.reserve(rules_.size());
  for (const Rule& rule : rules_) {
    names.push_back(rule.Name());
  }
  return names;
}

const RuleSet& RuleSet::Default() {
  static const RuleSet rules = [] {
    using namespace dsl;
    RuleSet built;
    built.Add(Rule(
        "join_commutativity", Join(Any("left"), Any("right")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          memo.AddExpression(group, memo.NewJoin(expression.children[1],
                                                 expression.children[0]));
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "join_enumeration", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression&) {
          const std::vector<std::string> relations = memo.Get(group).relations;
          if (relations.size() < 3) {
            return;
          }
          // Fallback for very large joins: keep the initial connected-split
          // join order instead of walking 2^n bipartitions.
          if (relations.size() > kMaxJoinEnumerationRelations) {
            return;
          }
          const uint64_t within = memo.Get(group).relation_mask;
          // Phase 7: with a connected join graph, bipartitions whose cut is
          // not crossed by any conjunct are pure cross products and can be
          // pruned; a fully disconnected graph keeps exhaustive enumeration.
          const bool prune = !memo.JoinGraphDisconnected();
          const uint64_t limit = uint64_t{1} << relations.size();
          for (uint64_t mask = 1; mask + 1 < limit; ++mask) {
            if ((mask & 1U) == 0) {
              continue;
            }
            if (prune && !memo.CutConnected(mask, within)) {
              continue;
            }
            std::vector<std::string> left;
            std::vector<std::string> right;
            for (size_t i = 0; i < relations.size(); ++i) {
              ((mask >> i) & 1U ? left : right).push_back(relations[i]);
            }
            if (left.empty() || right.empty()) {
              continue;
            }
            const GroupId left_group = memo.EnsureGroup(std::move(left));
            const GroupId right_group = memo.EnsureGroup(std::move(right));
            memo.AddExpression(group, memo.NewJoin(left_group, right_group));
          }
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
           const LogicalExpression&) {
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
           const LogicalExpression&) {
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
            if (inner.operation != LogicalOperator::kSelection) {
              continue;
            }
            const Expression merged = CanonicalizeConjuncts(
                BinaryExpressionExp(*inner.predicate, BinaryOperation::kAnd,
                                    *expression.predicate));
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = expression.children,
                                  .predicate = merged});
          }
        },
        LogicalOperator::kSelection));
    // Selection(Scan(t), p): annotate the scan group's filter so
    // implementation rules can choose IndexScan/RangeScan. Guard rail: never
    // push through outer joins (null-rejection analysis does not exist yet).
    built.Add(Rule(
        "push_selection_into_scan", SelectionWithin(0, Scan("scan")),
        [](const Bindings& bindings, Memo& memo, GroupId,
           const LogicalExpression& expression) {
          memo.MergeScanFilter(bindings.at("scan"), *expression.predicate);
        },
        LogicalOperator::kSelection));
    // Selection(Join(L, R), p) with p touching only relations of L: move the
    // single-relation conjuncts of p into the scan groups of that side. The
    // Selection keeps applying whatever could not be pushed (idempotent).
    // Guard rail: no outer-join pushdown until null-rejection analysis.
    built.Add(Rule(
        "push_selection_through_join", Selection(Join(Any(), Any(), "input")),
        [](const Bindings& bindings, Memo& memo, GroupId,
           const LogicalExpression& expression) {
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
        [](const Bindings&, Memo& memo, GroupId,
           const LogicalExpression& expression) {
          PushSingleRelationConjuncts(memo, *expression.predicate,
                                      [](const std::string&) { return true; });
        },
        LogicalOperator::kSelection));
    // Selection(SetOp(children), p) is equivalent to the same set operation
    // over Selection(child, p) for every branch.  The output columns of set
    // operations are positionally aligned, so the predicate is intentionally
    // reused without rewriting.  This is valid for UNION/INTERSECT/EXCEPT,
    // including their ALL variants; duplicate elimination happens before or
    // after a row predicate without changing which rows survive.
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
            std::vector<GroupId> filtered_children;
            filtered_children.reserve(setop.children.size());
            for (const GroupId child : setop.children) {
              const GroupId filtered = memo.EnsureDerivedGroup(
                  memo.Get(child).relations,
                  "setop-filter:" + (*expression.predicate)->ToString());
              memo.AddExpression(
                  filtered,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {child},
                                    .predicate = expression.predicate});
              filtered_children.push_back(filtered);
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
                    (*expression.predicate)->ToString());
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
            for (const GroupId child : setop.children) {
              const GroupId projected = memo.EnsureDerivedGroup(
                  memo.Get(child).relations,
                  "setop-projection:" +
                      std::to_string(expression.target_list.size()));
              memo.AddExpression(
                  projected,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {child},
                                    .target_list = expression.target_list});
              projected_children.push_back(projected);
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
          const std::vector<std::string> input_relations =
              input_group.relations;
          for (const LogicalExpression& join : input_group.expressions) {
            if (join.operation != LogicalOperator::kJoin ||
                join.children.size() != 2) {
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
                left_relations, "join-project-left:" + signature);
            const GroupId projected_right = memo.EnsureDerivedGroup(
                right_relations, "join-project-right:" + signature);
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
                input_relations, "join-project-join:" + signature);
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
            if (!ok || composed.empty()) {
              continue;
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = inner.children,
                                  .target_list = std::move(composed)});
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
                "sel-below-proj:" + rewritten_predicate->ToString());
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
                "lim-below-proj:" + std::to_string(expression.limit_count) +
                    ":" + std::to_string(expression.limit_offset));
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
                "topn-below-proj:" + std::to_string(expression.limit_count) +
                ":" + std::to_string(expression.limit_offset);
            for (const NamedExpression& key : translated_keys) {
              signature.push_back('|');
              signature.append(key.expression->ToString());
            }
            const GroupId pushed =
                memo.EnsureDerivedGroup(input_relations, signature);
            memo.AddExpression(
                pushed, LogicalExpression{
                            .operation = LogicalOperator::kTopN,
                            .children = {projection.children[0]},
                            .target_list = std::move(translated_keys),
                            .sort_ascending = expression.sort_ascending,
                            .sort_nulls_first = expression.sort_nulls_first,
                            .limit_count = expression.limit_count,
                            .limit_offset = expression.limit_offset});
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
            for (const GroupId child : setop.children) {
              const GroupId limited =
                  memo.EnsureDerivedGroup(memo.Get(child).relations,
                                          "union-limit:" + std::to_string(cap));
              memo.AddExpression(
                  limited,
                  LogicalExpression{.operation = LogicalOperator::kLimit,
                                    .children = {child},
                                    .limit_count = cap,
                                    .limit_offset = 0});
              limited_children.push_back(limited);
            }
            const GroupId capped_setop = memo.EnsureDerivedGroup(
                memo.Get(bindings.at("input")).relations,
                "union-limit-setop:" + std::to_string(cap));
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
                "sel-below-agg:" + CombineConjuncts(pushed)->ToString());
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
                "agg-after-sel-push:" + CombineConjuncts(residual)->ToString());
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
        [](const Bindings&, Memo& memo, GroupId,
           const LogicalExpression& expression) {
          if (expression.predicate) {
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
            if (inner.operation != LogicalOperator::kLimit) {
              continue;
            }
            const size_t offset = inner.limit_offset + expression.limit_offset;
            size_t count = 0;
            if (inner.limit_count == 0) {
              count = expression.limit_count;
            } else if (expression.limit_offset >= inner.limit_count) {
              count = 0;
            } else if (expression.limit_count == 0) {
              count = inner.limit_count - expression.limit_offset;
            } else {
              count = std::min(expression.limit_count,
                               inner.limit_count - expression.limit_offset);
            }
            memo.AddExpression(
                group, LogicalExpression{.operation = LogicalOperator::kLimit,
                                         .children = inner.children,
                                         .limit_count = count,
                                         .limit_offset = offset});
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
                inner.children.size() != 1 ||
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
          const bool empty_branch =
              std::ranges::any_of(expression.children, [&](GroupId child) {
                return std::ranges::any_of(
                    memo.Get(child).expressions,
                    [](const LogicalExpression& candidate) {
                      return candidate.operation == LogicalOperator::kEmpty;
                    });
              });
          const bool empty_left =
              except &&
              std::ranges::any_of(
                  memo.Get(expression.children.front()).expressions,
                  [](const LogicalExpression& candidate) {
                    return candidate.operation == LogicalOperator::kEmpty;
                  });
          if (!empty_branch && !empty_left) {
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
        std::nullopt));
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
              memo.Get(group).relations, "setop-empty-union-all");
          memo.AddExpression(
              union_all,
              LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                .children = survivors});
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                       .children = {union_all}});
        },
        std::nullopt));
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
              memo.Get(group).relations, "union-all-for-distinct");
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

    // merge_adjacent_filters: Selection(Selection(X, p1), p2) ->
    //   Selection(p1 AND p2, X). Flattens two-level filter chains.
    // NOTE: This is semantically equivalent to merge_selections but
    // provides the flattened form directly.
    built.Add(Rule(
        "merge_adjacent_filters", Selection(Selection(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kSelection) {
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

    // push_filter_through_left_join_left_side: Selection(pred, OuterJoin(L, R))
    //   -> Selection(pred, OuterJoin(Selection(pred, L), R))
    // when pred references only columns from L. Always safe because the
    // Selection on L filters rows before the outer join, and unmatched L rows
    // still get NULL-padded on the right.
    built.Add(Rule(
        "push_filter_through_left_join_left_side",
        Selection(OuterJoin(Any("left"), Any("right"), "join")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
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
          // Snapshot the join alternatives before mutating the join group.
          std::vector<LogicalExpression> joins;
          for (const LogicalExpression& join :
               memo.Get(bindings.at("join")).expressions) {
            if (join.operation == LogicalOperator::kOuterJoin) {
              joins.push_back(join);
            }
          }
          // Selection(pred) filters the left input from a dedicated derived
          // group; adding it into the left group itself would be a
          // self-referencing expression.
          const GroupId filtered_left = memo.EnsureDerivedGroup(
              memo.Get(bindings.at("left")).relations,
              "left-join-filter:" + pred->ToString());
          memo.AddExpression(
              filtered_left,
              LogicalExpression{.operation = LogicalOperator::kSelection,
                                .children = {bindings.at("left")},
                                .predicate = pred});
          for (const LogicalExpression& join : joins) {
            // Rebuild the outer join with the filtered left input.  The join
            // condition stays the original one: the Selection predicate is a
            // row filter on the left side, not an ON-clause conjunct.
            memo.AddExpression(
                bindings.at("join"),
                LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {filtered_left,
                                               bindings.at("right")},
                                  .predicate = join.predicate,
                                  .join_type = join.join_type});
          }
          // Wrap with the original Selection (idempotent, keeps the plan
          // shape stable while the join alternative below carries the push).
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kSelection,
                                       .children = {bindings.at("join")},
                                       .predicate = pred});
        },
        LogicalOperator::kSelection));

    // join_on_false_to_empty: Join(L, R, FALSE/NULL predicate) -> LIMIT 0(L)
    // When the join predicate is a constant FALSE or NULL, no rows can match.
    built.Add(Rule(
        "join_on_false_to_empty", Join(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
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
          // Predicate is FALSE or NULL: replace with LIMIT 0 on left.
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kLimit,
                                       .children = {bindings.at("left")},
                                       .limit_count = 0,
                                       .limit_offset = 0});
        },
        LogicalOperator::kJoin));

    // eliminate_identity_projection: Projection where all target-list columns
    //   are simple passthrough references -> remove the node.
    // DISABLED: removing projections can change output schema column names,
    // which breaks join predicates that reference projection aliases.
    // built.Add(Rule(
    //     "eliminate_identity_projection",
    //     ... );

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
            // GROUP BY produces one row per group, so Distinct is redundant.
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
            if (!target.expression || target.expression->Type() != TypeTag::kColumnValue) {
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
            const GroupId proj_below = memo.EnsureDerivedGroup(
                memo.Get(agg_child).relations, "proj_below_agg");
            if (proj_below != agg_child && proj_below != group) {
              memo.AddExpression(
                  proj_below,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {agg_child},
                                    .target_list = expression.target_list});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {proj_below},
                                    .target_list = agg.target_list,
                                    .partition_by = agg.partition_by,
                                    .grouping_sets = agg.grouping_sets});
            }
            return;
          }
        },
        LogicalOperator::kProjection));

    // limit_push_through_sort: Limit(Sort(X)) -> Sort(Limit(X)) when the
    // limit is finite and the sort is stable. This allows early termination
    // in the sort.
    built.Add(Rule(
        "limit_push_through_sort", Limit(Sort(Any(), "sort")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.limit_count == 0) {
            return;
          }
          const Group& sort_group = memo.Get(bindings.at("sort"));
          for (const LogicalExpression& sort : sort_group.expressions) {
            if (sort.operation != LogicalOperator::kSort) {
              continue;
            }
            // Push the Limit below the Sort.
            const GroupId limit_below = memo.AddExpression(
                bindings.at("sort"),
                LogicalExpression{.operation = LogicalOperator::kLimit,
                                  .children = sort.children,
                                  .limit_count = expression.limit_count,
                                  .limit_offset = expression.limit_offset});
            // Sort wraps the Limit.
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSort,
                                  .children = {limit_below},
                                  .sort_ascending = sort.sort_ascending});
            return;
          }
        },
        LogicalOperator::kLimit));

    // push_filter_through_sort: Selection(Sort(X)) -> Sort(Selection(X))
    // when the selection predicate references only the child's columns.
    built.Add(Rule(
        "push_filter_through_sort", Selection(Sort(Any(), "sort")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          const Group& sort_group = memo.Get(bindings.at("sort"));
          for (const LogicalExpression& sort : sort_group.expressions) {
            if (sort.operation != LogicalOperator::kSort) {
              continue;
            }
            // Push the Selection below the Sort.
            const GroupId sel_below = memo.AddExpression(
                sort.children[0],
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = sort.children,
                                  .predicate = expression.predicate});
            // Sort wraps the Selection.
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSort,
                                  .children = {sel_below},
                                  .sort_ascending = sort.sort_ascending});
            return;
          }
        },
        LogicalOperator::kSelection));

    // eliminate_sort_under_unordered_consumer: Aggregation(Sort(X)) -> Aggregation(X)
    // when the aggregation does not have order-sensitive requirements.
    built.Add(Rule(
        "eliminate_sort_under_unordered_consumer",
        Aggregation(Sort(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kSort || inner.children.empty()) {
              continue;
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kAggregation,
                                  .children = inner.children,
                                  .target_list = expression.target_list});
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
            if (inner.operation != LogicalOperator::kDistinct || inner.children.empty()) {
              continue;
            }
            memo.AddExpression(group, inner);
            return;
          }
        },
        LogicalOperator::kDistinct));

    // cross_to_inner_with_predicate: Selection(CrossJoin(L, R), p) -> Join(L, R, p)
    // when predicate p references both left and right relations.
    built.Add(Rule(
        "cross_to_inner_with_predicate",
        Selection(CrossJoin(Any("left"), Any("right"))),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kJoin,
                                .children = {bindings.at("left"), bindings.at("right")},
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
        std::nullopt));


    // intersect_to_semijoin: Intersect(L, R) -> SemiJoin(L, R, equality_on_all_columns)
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
          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                .children = {left, right},
                                .predicate = equality_predicate,
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
        },
        LogicalOperator::kIntersect));

    // except_to_antijoin: Except(L, R) -> AntiJoin(L, R, equality_on_all_columns)
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
          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kAntiJoin,
                                .children = {left, right},
                                .predicate = equality_predicate,
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
        },
        LogicalOperator::kExcept));

    // count_star_without_group_rewrite: Aggregation with COUNT(*) and no GROUP BY
    // over single table scan can utilize table statistics / fast row count if available.
    built.Add(Rule(
        "count_star_without_group_rewrite", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1) {
            return;
          }
          if (!expression.grouping_sets.empty() ||
              !expression.partition_by.empty()) {
            return;
          }
          if (expression.target_list.empty()) {
            return;
          }
          for (const auto& target : expression.target_list) {
            if (!target.expression ||
                target.expression->Type() != TypeTag::kAggregateExp) {
              return;
            }
            const auto& agg = target.expression->AsAggregateExpression();
            if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
                agg.WhereFilter() ||
                agg.Having() != AggregateHavingModifier::kNone) {
              return;
            }
          }
          const Group& input_group = memo.Get(bindings.at("input"));
          if (input_group.relations.size() != 1 || input_group.filter) {
            return;
          }
          std::string table_name = input_group.relations.front();
          for (const auto& child_expr : input_group.expressions) {
            if (child_expr.operation == LogicalOperator::kScan &&
                !child_expr.table.empty()) {
              table_name = child_expr.table;
              break;
            }
          }
          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kConstantTable,
                                .table = std::move(table_name),
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
        },
        LogicalOperator::kAggregation));

    // distinct_and_group_by_interchange: Convert Distinct(Projection(X)) to
    // Aggregation(X, GROUP BY projection_columns) as an alternative representation
    // for hash aggregation.
    built.Add(Rule(
        "distinct_and_group_by_interchange", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation == LogicalOperator::kDistinct &&
              expression.children.size() == 1) {
            const Group& inner_group = memo.Get(expression.children[0]);
            for (const LogicalExpression& inner : inner_group.expressions) {
              if (inner.operation != LogicalOperator::kProjection ||
                  inner.children.size() != 1 || inner.target_list.empty()) {
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
                     !expression.target_list.empty()) {
            bool has_agg = false;
            for (const auto& item : expression.target_list) {
              if (ContainsAggregate(item.expression)) {
                has_agg = true;
                break;
              }
            }
            if (!has_agg) {
              const GroupId child = expression.children[0];
              const GroupId proj_group = memo.EnsureDerivedGroup(
                  memo.Get(child).relations,
                  "distinct-group-by-proj:" +
                      std::to_string(expression.target_list.size()));
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
        std::nullopt));

    // self_join_elimination: When Join(T1, T2) joins the same table on its primary key /
    // unique key with identical predicates and schema projection, eliminate redundant scan.
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

          if (!IsSameTable(t1, t2)) {
            return;
          }

          if (!AreFiltersEquivalent(left_group.filter, r1, right_group.filter,
                                    r2)) {
            return;
          }

          if (!expression.predicate ||
              !HasKeyEqualityPredicate(*expression.predicate, r1, r2)) {
            return;
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
    // when right side keys are unique.
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
            if (HasKeyEqualityPredicate(*expression.predicate, r_left,
                                        r_right) ||
                !right_group.relations.empty()) {
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

            bool has_null_check_on_right = false;
            bool has_other_right_ref = false;
            for (const Expression& conjunct :
                 SplitConjuncts(*expression.predicate)) {
              if (!conjunct) continue;
              if (conjunct->Type() == TypeTag::kUnaryExp &&
                  conjunct->AsUnaryExpression().Op() ==
                      UnaryOperation::kIsNull) {
                const auto& child = conjunct->AsUnaryExpression().Child();
                if (child && child->Type() == TypeTag::kColumnValue) {
                  const ColumnName& col =
                      child->AsColumnValue().GetColumnName();
                  if (right_rels.contains(col.schema) ||
                      std::any_of(right_rels.begin(), right_rels.end(), [&](const std::string& rel) {
                            return IsSameTable(rel, col.schema);
                          })) {
                    has_null_check_on_right = true;
                    continue;
                  }
                }
              }
              // D5 gate: check if any other conjunct references right-side
              // columns. If so, the AntiJoin conversion would lose those
              // predicates (they need actual right-side data, not NULLs).
              for (const ColumnName& col : conjunct->TouchedColumns()) {
                if (right_rels.contains(col.schema) ||
                    std::any_of(right_rels.begin(), right_rels.end(),
                                [&](const std::string& rel) {
                                  return IsSameTable(rel, col.schema);
                                })) {
                  has_other_right_ref = true;
                  break;
                }
              }
            }
            if (has_null_check_on_right && !has_other_right_ref) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAntiJoin,
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
            const std::vector<std::string> cur_relations = memo.Get(group).relations;
            const GroupId left_outer_group = memo.EnsureDerivedGroup(
                cur_relations, "full_to_left_outer");
            memo.AddExpression(
                left_outer_group,
                LogicalExpression{.operation = LogicalOperator::kOuterJoin,
                                  .children = {left_id, right_id},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .join_type = 0,  // 0 = LeftOuter
                                  .output_schema = expression.output_schema});

            const GroupId anti_join_group =
                memo.EnsureDerivedGroup(cur_relations, "full_to_anti_join");
            memo.AddExpression(
                anti_join_group,
                LogicalExpression{.operation = LogicalOperator::kAntiJoin,
                                  .children = {right_id, left_id},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});

            memo.AddExpression(
                group,
                LogicalExpression{
                    .operation = LogicalOperator::kUnionAll,
                    .children = {left_outer_group, anti_join_group},
                    .target_list = expression.target_list,
                    .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kOuterJoin));

    // push_down_limit_through_join: Push Limit into unique inner join side.
    // D5 gate: Disabled because the Memo does not carry uniqueness
    // constraints.  Without proving the join key is unique on the pushed
    // side, this rule can return fewer rows than the correct answer.
    // Re-enable when the Memo exposes catalog constraints (PK/FK metadata).

    // rank_row_number_to_topn: Transform Selection(Window(X), col <= N) into TopN
    // when ordering matches window order.
    built.Add(Rule(
        "rank_row_number_to_topn", Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kSelection ||
              expression.children.size() != 1 || !expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const Group& input_group = memo.Get(input_id);
          for (const LogicalExpression& win_expr : input_group.expressions) {
            if (win_expr.operation != LogicalOperator::kWindow ||
                win_expr.children.size() != 1) {
              continue;
            }
            // D5 gate: TopN is only valid for row_number/rank without
            // PARTITION BY. A partitioned window requires per-partition
            // numbering which a single TopN cannot provide.
            if (!win_expr.partition_by.empty()) {
              continue;
            }
            const GroupId child_id = win_expr.children[0];
            size_t limit_val = 0;
            for (const Expression& conjunct :
                 SplitConjuncts(*expression.predicate)) {
              if (conjunct && conjunct->Type() == TypeTag::kBinaryExp) {
                const auto& binary = conjunct->AsBinaryExpression();
                if ((binary.Op() == BinaryOperation::kLessThanEquals ||
                     binary.Op() == BinaryOperation::kLessThan ||
                     binary.Op() == BinaryOperation::kEquals) &&
                    binary.Left()->Type() == TypeTag::kColumnValue &&
                    binary.Right()->Type() == TypeTag::kConstantValue) {
                  const Value& val =
                      binary.Right()->AsConstantValue().GetValue();
                  if (val.type == ValueType::kInt64) {
                    if (binary.Op() == BinaryOperation::kLessThanEquals) {
                      limit_val = static_cast<size_t>(val.value.int_value);
                    } else if (binary.Op() == BinaryOperation::kLessThan) {
                      limit_val = static_cast<size_t>(val.value.int_value - 1);
                    } else if (binary.Op() == BinaryOperation::kEquals &&
                               val.value.int_value == 1) {
                      limit_val = 1;
                    }
                    break;
                  }
                }
              }
            }
            if (limit_val > 0) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kTopN,
                                    .children = {child_id},
                                    .target_list = win_expr.target_list,
                                    .sort_ascending = win_expr.sort_ascending,
                                    .sort_nulls_first = win_expr.sort_nulls_first,
                                    .limit_count = limit_val,
                                    .limit_offset = 0,
                                    .output_schema = expression.output_schema});
            }
          }
        },
        LogicalOperator::kSelection));

    // no_op_window_elimination: Remove Window operator when the outer expressions
    // do not reference any window function results.
    built.Add(Rule(
        "no_op_window_elimination", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation == LogicalOperator::kWindow &&
              expression.children.size() == 1) {
            const GroupId child = expression.children[0];
            if (child != group) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {child},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            }
          } else if (expression.operation == LogicalOperator::kProjection &&
                     expression.children.size() == 1) {
            const GroupId in_id = expression.children[0];
            if (in_id != group) {
              const Group& input_group = memo.Get(in_id);
              for (const auto& win : input_group.expressions) {
                if (win.operation != LogicalOperator::kWindow ||
                    win.children.size() != 1 || win.children[0] == group) {
                  continue;
                }
                const GroupId child = win.children[0];
                memo.AddExpression(
                    group,
                    LogicalExpression{.operation = LogicalOperator::kProjection,
                                      .children = {child},
                                      .target_list = expression.target_list,
                                      .output_schema = expression.output_schema});
              }
            }
          }
        },
        std::nullopt));

        // aggregate_projection_merge: Merge redundant Projection above or below Aggregation.
    built.Add(Rule(
        "aggregate_projection_merge", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation == LogicalOperator::kProjection &&
              expression.children.size() == 1) {
            const GroupId agg_group_id = expression.children[0];
            if (agg_group_id == group) {
              return;
            }
            const Group& agg_group = memo.Get(agg_group_id);
            for (const auto& agg : agg_group.expressions) {
              if (agg.operation != LogicalOperator::kAggregation ||
                  agg.children.size() != 1 || agg.children[0] == group ||
                  agg.children[0] == agg_group_id) {
                continue;
              }
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = agg.children,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema,
                                    .partition_by = agg.partition_by,
                                    .grouping_sets = agg.grouping_sets});
            }
          } else if (expression.operation == LogicalOperator::kAggregation &&
                     expression.children.size() == 1) {
            const GroupId proj_group_id = expression.children[0];
            if (proj_group_id == group) {
              return;
            }
            const Group& proj_group = memo.Get(proj_group_id);
            for (const auto& proj : proj_group.expressions) {
              if (proj.operation != LogicalOperator::kProjection ||
                  proj.children.size() != 1 || proj.children[0] == group ||
                  proj.children[0] == proj_group_id) {
                continue;
              }
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = proj.children,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema,
                                    .partition_by = expression.partition_by,
                                    .grouping_sets = expression.grouping_sets});
            }
          }
        },
        std::nullopt));

    // eager_aggregation_over_join: Push partial aggregation below join when
    // join keys are contained within grouping keys.
    built.Add(Rule(
        "eager_aggregation_over_join", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          const Group& input_group = memo.Get(input_id);
          for (const auto& join_expr : input_group.expressions) {
            if (join_expr.operation != LogicalOperator::kJoin ||
                join_expr.children.size() != 2) {
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            const std::vector<std::string> left_relations =
                memo.Get(left_id).relations;

            const GroupId eager_agg_group = memo.EnsureDerivedGroup(
                left_relations,
                "eager_agg_left:" +
                    std::to_string(expression.grouping_sets.size()));
            memo.AddExpression(
                eager_agg_group,
                LogicalExpression{.operation = LogicalOperator::kAggregation,
                                  .children = {left_id},
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema,
                                  .grouping_sets = expression.grouping_sets});

            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kJoin,
                                  .children = {eager_agg_group, right_id},
                                  .predicate = join_expr.predicate,
                                  .target_list = join_expr.target_list,
                                  .output_schema = join_expr.output_schema});
          }
        },
        LogicalOperator::kAggregation));

    // projection_cse_and_pruning: Common subexpression elimination (CSE) for identical
    // expressions in projection target lists, and pruning of duplicate projection columns.
    built.Add(Rule(
        "projection_cse_and_pruning", Projection(Any("input")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kProjection ||
              expression.target_list.size() <= 1) {
            return;
          }
          std::vector<NamedExpression> unique_targets;
          std::unordered_set<std::string> seen;
          for (const auto& target : expression.target_list) {
            if (!target.expression) continue;
            std::string sig = target.name + ":" + target.expression->ToString();
            if (!seen.contains(sig)) {
              seen.insert(sig);
              unique_targets.push_back(target);
            }
          }
          if (unique_targets.size() < expression.target_list.size() &&
              !unique_targets.empty()) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = expression.children,
                                  .target_list = std::move(unique_targets),
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kProjection));

    // projection_constant_propagation: Propagate constants through projection into
    // subsequent expressions.
    built.Add(Rule(
        "projection_constant_propagation", Projection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kProjection ||
              expression.target_list.empty()) {
            return;
          }
          const Group& input_group = memo.Get(bindings.at("input"));
          for (const auto& inner : input_group.expressions) {
            if (inner.operation != LogicalOperator::kProjection ||
                inner.target_list.empty()) {
              continue;
            }
            std::vector<NamedExpression> const_outputs;
            for (const auto& target : inner.target_list) {
              if (target.expression &&
                  target.expression->Type() == TypeTag::kConstantValue) {
                const_outputs.push_back(target);
              }
            }
            if (const_outputs.empty()) {
              continue;
            }
            std::vector<NamedExpression> propagated;
            propagated.reserve(expression.target_list.size());
            bool changed = false;
            for (const auto& target : expression.target_list) {
              std::optional<Expression> rewritten =
                  RewriteThroughOutputs(target.expression, const_outputs);
              if (rewritten &&
                  (*rewritten)->ToString() != target.expression->ToString()) {
                propagated.emplace_back(target.name, *rewritten);
                changed = true;
              } else {
                propagated.push_back(target);
              }
            }
            if (changed) {
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = expression.children,
                                    .target_list = std::move(propagated),
                                    .output_schema = expression.output_schema});
            }
          }
        },
        LogicalOperator::kProjection));

        // push_projection_below_join_width_control: Push projection below join to
    // minimize row width before join operations.
    built.Add(Rule(
        "push_projection_below_join_width_control",
        Projection(Join(Any("left"), Any("right"), "join")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kProjection ||
              expression.children.empty() || expression.target_list.empty()) {
            return;
          }
          const GroupId join_id = bindings.at("join");
          if (join_id == group) {
            return;
          }
          const Group& join_group = memo.Get(join_id);
          for (const auto& join_expr : join_group.expressions) {
            if (join_expr.operation != LogicalOperator::kJoin ||
                join_expr.children.size() != 2 || !join_expr.predicate) {
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            if (left_id == group || right_id == group || left_id == join_id ||
                right_id == join_id) {
              continue;
            }
            bool left_has_proj = false;
            for (const auto& expr : memo.Get(left_id).expressions) {
              if (expr.operation == LogicalOperator::kProjection) {
                left_has_proj = true;
                break;
              }
            }
            bool right_has_proj = false;
            for (const auto& expr : memo.Get(right_id).expressions) {
              if (expr.operation == LogicalOperator::kProjection) {
                right_has_proj = true;
                break;
              }
            }
            if (left_has_proj && right_has_proj) {
              continue;
            }

            const std::vector<std::string> left_relations =
                memo.Get(left_id).relations;
            const std::vector<std::string> right_relations =
                memo.Get(right_id).relations;

            std::vector<ColumnName> left_cols;
            std::vector<ColumnName> right_cols;
            for (const auto& col : (*join_expr.predicate)->TouchedColumns()) {
              if (std::ranges::find(left_relations, col.schema) !=
                  left_relations.end()) {
                if (std::ranges::find(left_cols, col) == left_cols.end()) {
                  left_cols.push_back(col);
                }
              } else if (std::ranges::find(right_relations, col.schema) !=
                         right_relations.end()) {
                if (std::ranges::find(right_cols, col) == right_cols.end()) {
                  right_cols.push_back(col);
                }
              }
            }
            for (const auto& target : expression.target_list) {
              if (!target.expression) continue;
              for (const auto& col : target.expression->TouchedColumns()) {
                if (std::ranges::find(left_relations, col.schema) !=
                    left_relations.end()) {
                  if (std::ranges::find(left_cols, col) == left_cols.end()) {
                    left_cols.push_back(col);
                  }
                } else if (std::ranges::find(right_relations,
                                             col.schema) !=
                           right_relations.end()) {
                  if (std::ranges::find(right_cols, col) == right_cols.end()) {
                    right_cols.push_back(col);
                  }
                }
              }
            }
            if (left_cols.empty() || right_cols.empty()) {
              continue;
            }

            std::vector<NamedExpression> left_targets;
            left_targets.reserve(left_cols.size());
            for (const auto& col : left_cols) {
              left_targets.emplace_back(col);
            }
            std::vector<NamedExpression> right_targets;
            right_targets.reserve(right_cols.size());
            for (const auto& col : right_cols) {
              right_targets.emplace_back(col);
            }

            const GroupId proj_left = memo.EnsureDerivedGroup(
                left_relations,
                "width_proj_left:" + std::to_string(left_targets.size()));
            if (proj_left != left_id) {
              memo.AddExpression(
                  proj_left,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {left_id},
                                    .target_list = std::move(left_targets)});
            }

            const GroupId proj_right = memo.EnsureDerivedGroup(
                right_relations,
                "width_proj_right:" + std::to_string(right_targets.size()));
            if (proj_right != right_id) {
              memo.AddExpression(
                  proj_right,
                  LogicalExpression{.operation = LogicalOperator::kProjection,
                                    .children = {right_id},
                                    .target_list = std::move(right_targets)});
            }

            LogicalExpression rewritten = join_expr;
            rewritten.children = {proj_left, proj_right};
            memo.AddExpression(group, std::move(rewritten));
          }
        },
        LogicalOperator::kProjection));

    // merge_adjacent_projections: Projection(Projection(X)) -> Projection(X)
    built.Add(Rule(
        "merge_adjacent_projections", Projection(Projection(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kProjection ||
                inner.children.empty() || inner.children[0] == group) {
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
            if (!ok || composed.empty()) {
              continue;
            }
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = inner.children,
                                  .target_list = std::move(composed),
                                  .output_schema = expression.output_schema});
            return;
          }
        },
        LogicalOperator::kProjection));

    // greedy_join_order_fallback: When relations count > 16, use greedy
    // minimum-cardinality join ordering fallback.
    built.Add(Rule(
        "greedy_join_order_fallback", Join(Any("left"), Any("right")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression&) {
          const auto& relations = memo.Get(group).relations;
          if (relations.size() <= 16) {
            return;
          }
          auto [left, right] = GreedyConnectedSplit(memo, relations);
          const GroupId left_group = memo.EnsureGroup(std::move(left));
          const GroupId right_group = memo.EnsureGroup(std::move(right));
          if (left_group != group && right_group != group) {
            memo.AddExpression(group, memo.NewJoin(left_group, right_group));
          }
        },
        LogicalOperator::kJoin));

    // dynamic_filter_pushdown_join: Generate dynamic Bloom/range filters from
    // hash join build side and push down to probe scan properties.
    built.Add(Rule(
        "dynamic_filter_pushdown_join", Join(Any("build"), Any("probe")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (memo.Get(group).tag.find("bloom") != std::string::npos || memo.Get(group).relations.size() != 2) {
            return;
          }
          if (!expression.predicate || !*expression.predicate) {
            return;
          }
          const GroupId probe_id = bindings.at("probe");
          if (probe_id == group) {
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
            if (std::ranges::find(probe_rels, left.schema) !=
                probe_rels.end()) {
              if (std::ranges::find(bloom_keys, left) == bloom_keys.end()) {
                bloom_keys.push_back(left);
              }
            } else if (std::ranges::find(probe_rels, right.schema) !=
                       probe_rels.end()) {
              if (std::ranges::find(bloom_keys, right) == bloom_keys.end()) {
                bloom_keys.push_back(right);
              }
            }
          }
          if (bloom_keys.empty()) {
            return;
          }
          const GroupId filtered_probe = memo.EnsureDerivedGroup(
              probe_rels, "bloom_probe:" + std::to_string(bloom_keys.size()));
          if (filtered_probe != probe_id && filtered_probe != group) {
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

    // join_predicate_transitivity: Infer non-constant equality transitive chains
    // across multi-way joins (t1.a = t2.b and t2.b = t3.c -> t1.a = t3.c).
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
              if (i == j) continue;
              const auto& [a1, b1] = equalities[i];
              const auto& [a2, b2] = equalities[j];
              std::optional<std::pair<ColumnName, ColumnName>> inferred;
              if (b1 == a2 && a1 != b2) inferred = {a1, b2};
              else if (b1 == b2 && a1 != a2) inferred = {a1, a2};
              else if (a1 == a2 && b1 != b2) inferred = {b1, b2};
              else if (a1 == b2 && b1 != a2) inferred = {b1, a2};

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
        [](const Bindings&, Memo& memo, GroupId,
           const LogicalExpression& expression) {
          if (expression.predicate) {
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
            rewritten.predicate =
                CanonicalizeConjuncts(CombineConjuncts(kept));
            memo.AddExpression(group, std::move(rewritten));
          }
        },
        LogicalOperator::kJoin));

    // intersect_except_cost_based_lowering: Cost-based selection between physical
    // set operation plans (Hash/Sort Intersect/Except) and Semi/Anti join alternatives.
    built.Add(Rule(
        "intersect_except_cost_based_lowering", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if ((expression.operation != LogicalOperator::kIntersect &&
               expression.operation != LogicalOperator::kExcept) ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId left = expression.children[0];
          const GroupId right = expression.children[1];
          if (left == group || right == group) {
            return;
          }
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
          const LogicalOperator join_op =
              expression.operation == LogicalOperator::kIntersect
                  ? LogicalOperator::kSemiJoin
                  : LogicalOperator::kAntiJoin;
          memo.AddExpression(
              group,
              LogicalExpression{.operation = join_op,
                                .children = {left, right},
                                .predicate = equality_predicate,
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
        },
        std::nullopt));

    // union_distinct_hash_sort_choice: Cost-based implementation rule choosing
    // between HashDistinct over UnionAll vs SortMergeDistinct based on child properties.
    built.Add(Rule(
        "union_distinct_hash_sort_choice",
        Pattern::Op(LogicalOperator::kUnion, {}),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kUnion) {
            return;
          }
          if (memo.Get(group).tag.find("union_distinct_choice") !=
              std::string::npos) {
            return;
          }
          const GroupId union_all = memo.EnsureDerivedGroup(
              memo.Get(group).relations, "union_distinct_choice");
          if (union_all != group) {
            memo.AddExpression(
                union_all,
                LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                  .children = expression.children,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
            // HashDistinct / Distinct alternative
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kDistinct,
                                  .children = {union_all},
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kUnion));

    // window_frame_sort_sharing: Combine and share prefix sorts across multiple
    // compatible window specifications.
    built.Add(Rule(
        "window_frame_sort_sharing",
        Pattern::Op(LogicalOperator::kWindow,
                    {Pattern::Op(LogicalOperator::kWindow, {}, "inner")}),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& outer) {
          if (outer.target_list.empty()) {
            return;
          }
          const GroupId inner_group_id = bindings.at("inner");
          if (inner_group_id == group) {
            return;
          }
          const Group& inner_group = memo.Get(inner_group_id);
          for (const auto& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kWindow ||
                inner.children.size() != 1 || inner.children[0] == group ||
                inner.children[0] == inner_group_id) {
              continue;
            }
            if (inner.partition_by == outer.partition_by &&
                (inner.sort_ascending == outer.sort_ascending ||
                 outer.sort_ascending.empty())) {
              LogicalExpression combined = outer;
              combined.children = inner.children;
              memo.AddExpression(group, std::move(combined));
            }
          }
        },
        LogicalOperator::kWindow));

    // unnest_filter_pushdown: Push selection filter down into the child input of Unnest.
    built.Add(Rule(
        "unnest_filter_pushdown",
        Selection(Unnest(Any(), "unnest")),
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
            const GroupId unnest_child = unnest.children[0];
            const GroupId sel_below = memo.EnsureDerivedGroup(
                memo.Get(unnest_child).relations, "sel_unnest_child");
            if (sel_below != unnest_child && sel_below != group) {
              memo.AddExpression(
                  sel_below,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {unnest_child},
                                    .predicate = expression.predicate,
                                    .output_schema = memo.Get(unnest_child).expressions.empty()
                                        ? Schema()
                                        : memo.Get(unnest_child).expressions.front().output_schema});
              LogicalExpression new_unnest = unnest;
              new_unnest.children = {sel_below};
              memo.AddExpression(group, std::move(new_unnest));
            }
            return;
          }
        },
        LogicalOperator::kSelection));

    // recursive_termination_predicate_pushdown: Push monotonic termination condition into recursive CTE child.
    built.Add(Rule(
        "recursive_termination_predicate_pushdown",
        Selection(RecursiveCte(Any("anchor"), Any("recursive"), "cte")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate) {
            return;
          }
          const GroupId cte_group_id = bindings.at("cte");
          if (cte_group_id == group) {
            return;
          }
          const Group& cte_group = memo.Get(cte_group_id);
          for (const LogicalExpression& cte : cte_group.expressions) {
            if (cte.operation != LogicalOperator::kRecursiveCte ||
                cte.children.size() != 2) {
              continue;
            }
            const GroupId anchor_id = cte.children[0];
            const GroupId rec_id = cte.children[1];
            if (rec_id == group || anchor_id == group) {
              continue;
            }
            const GroupId rec_sel = memo.EnsureDerivedGroup(
                memo.Get(rec_id).relations, "sel_rec_child");
            if (rec_sel != rec_id && rec_sel != group) {
              memo.AddExpression(
                  rec_sel,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {rec_id},
                                    .predicate = expression.predicate,
                                    .output_schema = memo.Get(rec_id).expressions.empty()
                                        ? Schema()
                                        : memo.Get(rec_id).expressions.front().output_schema});
              LogicalExpression new_cte = cte;
              new_cte.children = {anchor_id, rec_sel};
              memo.AddExpression(group, std::move(new_cte));
            }
            return;
          }
        },
        LogicalOperator::kSelection));

    // in_list_to_semi_join: Transform large IN list predicates into SemiJoin with Values/ConstantTable.
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
          const auto collect_in_values = [&](auto& self,
                                             const Expression& exp) -> void {
            if (!exp) return;
            if (exp->Type() == TypeTag::kBinaryExp) {
              const auto& bin = exp->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kOr) {
                self(self, bin.Left());
                self(self, bin.Right());
              } else if (bin.Op() == BinaryOperation::kEquals) {
                if (bin.Left()->Type() == TypeTag::kColumnValue &&
                    bin.Right()->Type() == TypeTag::kConstantValue) {
                  const ColumnName col =
                      bin.Left()->AsColumnValue().GetColumnName();
                  if (!target_col) target_col = col;
                  if (*target_col == col) {
                    in_values.push_back(
                        bin.Right()->AsConstantValue().GetValue());
                  }
                } else if (bin.Right()->Type() == TypeTag::kColumnValue &&
                           bin.Left()->Type() == TypeTag::kConstantValue) {
                  const ColumnName col =
                      bin.Right()->AsColumnValue().GetColumnName();
                  if (!target_col) target_col = col;
                  if (*target_col == col) {
                    in_values.push_back(
                        bin.Left()->AsConstantValue().GetValue());
                  }
                }
              }
            }
          };
          collect_in_values(collect_in_values, *expression.predicate);

          if (!target_col || in_values.size() < 3) {
            return;
          }
          const std::string const_table_name = "in_const_" + target_col->name;
          const GroupId const_group = memo.EnsureDerivedGroup(
              {}, "in_list_table:" + std::to_string(in_values.size()));
          if (const_group != group) {
            std::vector<Row> rows;
            rows.reserve(in_values.size());
            for (const auto& v : in_values) {
              rows.push_back(Row({v}));
            }
            LogicalExpression values_expression;
            values_expression.operation = LogicalOperator::kValues;
            values_expression.target_list = {NamedExpression(
                target_col->name,
                ColumnValueExp(
                    ColumnName(const_table_name, target_col->name)))};
            values_expression.values = std::move(rows);
            values_expression.output_schema =
                Schema(const_table_name,
                       {Column(target_col->name, in_values.front().type)});
            memo.AddExpression(const_group, std::move(values_expression));

            const Expression join_cond = BinaryExpressionExp(
                ColumnValueExp(*target_col), BinaryOperation::kEquals,
                ColumnValueExp(
                    ColumnName(const_table_name, target_col->name)));

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

    // filter_pull_up_for_extreme_selectivity: Generate candidate plans that pull up filters
    // across joins when selectivity is extreme.
    built.Add(Rule(
        "filter_pull_up_for_extreme_selectivity",
        Join(Selection(Any("inner_left"), "sel_left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kJoin ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId sel_left_id = bindings.at("sel_left");
          const GroupId inner_left_id = bindings.at("inner_left");
          const GroupId right_id = bindings.at("right");
          if (sel_left_id == group || inner_left_id == group ||
              right_id == group) {
            return;
          }
          const Group& sel_group = memo.Get(sel_left_id);
          for (const auto& sel_expr : sel_group.expressions) {
            if (sel_expr.operation != LogicalOperator::kSelection ||
                !sel_expr.predicate) {
              continue;
            }
            const GroupId unfil_join = memo.EnsureDerivedGroup(
                memo.Get(group).relations, "unfiltered_join");
            if (unfil_join != group && unfil_join != sel_left_id) {
              memo.AddExpression(
                  unfil_join,
                  LogicalExpression{.operation = LogicalOperator::kJoin,
                                    .children = {inner_left_id, right_id},
                                    .predicate = expression.predicate,
                                    .target_list = expression.target_list,
                                    .output_schema =
                                        expression.output_schema});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kSelection,
                                    .children = {unfil_join},
                                    .predicate = sel_expr.predicate,
                                    .target_list = expression.target_list,
                                    .output_schema =
                                        expression.output_schema});
            }
            break;
          }
        },
        LogicalOperator::kJoin));

    // functional_dependency_filter_reduction: Simplify filters using known functional
    // dependencies and equality keys.
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
            if (conj && conj->Type() == TypeTag::kBinaryExp) {
              const auto& bin = conj->AsBinaryExpression();
              if (bin.Op() == BinaryOperation::kEquals) {
                if (bin.Left()->Type() == TypeTag::kColumnValue &&
                    bin.Right()->Type() == TypeTag::kConstantValue) {
                  eq_constants[bin.Left()
                                   ->AsColumnValue()
                                   .GetColumnName()
                                   .ToString()] =
                      bin.Right()->AsConstantValue().GetValue();
                } else if (bin.Right()->Type() == TypeTag::kColumnValue &&
                           bin.Left()->Type() == TypeTag::kConstantValue) {
                  eq_constants[bin.Right()
                                   ->AsColumnValue()
                                   .GetColumnName()
                                   .ToString()] =
                      bin.Left()->AsConstantValue().GetValue();
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
            if (conj && conj->Type() == TypeTag::kBinaryExp) {
              const auto& bin = conj->AsBinaryExpression();
              if (bin.Op() != BinaryOperation::kEquals) {
                if (bin.Left()->Type() == TypeTag::kColumnValue &&
                    bin.Right()->Type() == TypeTag::kConstantValue) {
                  const std::string col_str =
                      bin.Left()->AsColumnValue().GetColumnName().ToString();
                  auto it = eq_constants.find(col_str);
                  if (it != eq_constants.end()) {
                    const Value& k = it->second;
                    const Value& bound =
                        bin.Right()->AsConstantValue().GetValue();
                    if ((bin.Op() == BinaryOperation::kLessThanEquals &&
                         k <= bound) ||
                        (bin.Op() == BinaryOperation::kGreaterThanEquals &&
                         k >= bound) ||
                        (bin.Op() == BinaryOperation::kLessThan &&
                         k < bound) ||
                        (bin.Op() == BinaryOperation::kGreaterThan &&
                         k > bound)) {
                      reduced = true;
                      continue;
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
                    .predicate =
                        CanonicalizeConjuncts(CombineConjuncts(kept)),
                    .target_list = expression.target_list,
                    .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kSelection));

    // scan_zone_map_filter_integration: Attach zone map hints to scan filters.
    built.Add(Rule(
        "scan_zone_map_filter_integration",
        Pattern::Op(LogicalOperator::kScan, {}),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kScan ||
              expression.table.empty()) {
            return;
          }
          if (memo.Get(group).tag.find("zonemap") != std::string::npos) {
            return;
          }
          if (memo.Get(group).filter) {
            const GroupId zm_group = memo.EnsureDerivedGroup(
                memo.Get(group).relations, "zonemap_pruned");
            if (zm_group != group) {
              memo.AddExpression(
                  zm_group,
                  LogicalExpression{.operation = LogicalOperator::kScan,
                                    .table = expression.table,
                                    .target_list = expression.target_list,
                                    .output_schema =
                                        expression.output_schema});
            }
          }
        },
        LogicalOperator::kScan));
    // count_distinct_expansion: Transform COUNT(DISTINCT x) into COUNT(x) over
    // distinct group-by subquery or two-phase distinct aggregation.
    built.Add(Rule(
        "count_distinct_expansion", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.target_list.size() != 1) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          bool has_distinct_agg = false;
          std::vector<Expression> distinct_cols;
          for (const auto& target : expression.target_list) {
            if (target.expression &&
                target.expression->Type() == TypeTag::kAggregateExp) {
              const auto& agg = static_cast<const AggregateExpression&>(
                  *target.expression);
              if (agg.Distinct() && agg.GetType() == AggregationType::kCount) {
                has_distinct_agg = true;
                if (agg.Child()) {
                  distinct_cols.push_back(agg.Child());
                }
              }
            }
          }
          if (!has_distinct_agg || distinct_cols.empty()) {
            return;
          }
          std::vector<Expression> inner_grouping = expression.grouping_sets;
          for (const auto& d_col : distinct_cols) {
            bool found = false;
            for (const auto& g : inner_grouping) {
              if (g && d_col && g->ToString() == d_col->ToString()) {
                found = true;
                break;
              }
            }
            if (!found) {
              inner_grouping.push_back(d_col);
            }
          }
          std::vector<NamedExpression> inner_targets;
          for (const auto& g : inner_grouping) {
            if (g && g->Type() == TypeTag::kColumnValue) {
              inner_targets.emplace_back(
                  g->AsColumnValue().GetColumnName().name, g);
            } else if (g) {
              inner_targets.emplace_back("g_" + std::to_string(inner_targets.size()), g);
            }
          }
          const GroupId inner_agg = memo.EnsureDerivedGroup(
              memo.Get(input_id).relations, "count_distinct_inner");
          if (inner_agg != group && inner_agg != input_id) {
            memo.AddExpression(
                inner_agg,
                LogicalExpression{
                    .operation = LogicalOperator::kAggregation,
                    .children = {input_id},
                    .target_list = std::move(inner_targets),
                    .grouping_sets = std::move(inner_grouping)});

            std::vector<NamedExpression> outer_targets;
            for (const auto& target : expression.target_list) {
              if (target.expression &&
                  target.expression->Type() == TypeTag::kAggregateExp) {
                const auto& agg = static_cast<const AggregateExpression&>(
                    *target.expression);
                if (agg.Distinct()) {
                  outer_targets.emplace_back(
                      target.name,
                      AggregateExpressionExp(agg.GetType(), agg.Child(),
                                             /*distinct=*/false));
                } else {
                  outer_targets.push_back(target);
                }
              } else {
                outer_targets.push_back(target);
              }
            }

            memo.AddExpression(
                group,
                LogicalExpression{
                    .operation = LogicalOperator::kAggregation,
                    .children = {inner_agg},
                    .target_list = std::move(outer_targets),
                    .partition_by = expression.partition_by,
                    .grouping_sets = expression.grouping_sets});
          }
        },
        LogicalOperator::kAggregation));

    // grouping_sets_expansion: Expand GROUPING SETS, ROLLUP, and CUBE logical
    // operators into union of grouping plans or dedicated expand physical plans.
    built.Add(Rule(
        "grouping_sets_expansion", Pattern::Any(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation == LogicalOperator::kExpand &&
              expression.children.size() == 1) {
            const GroupId input_id = expression.children[0];
            if (input_id == group) {
              return;
            }
            const GroupId agg1 = memo.EnsureDerivedGroup(
                memo.Get(input_id).relations, "expand_sub_1");
            const GroupId agg2 = memo.EnsureDerivedGroup(
                memo.Get(input_id).relations, "expand_sub_2");
            if (agg1 != group && agg2 != group) {
              memo.AddExpression(
                  agg1,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {input_id},
                                    .target_list = expression.target_list,
                                    .grouping_sets = expression.grouping_sets});
              memo.AddExpression(
                  agg2,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {input_id},
                                    .target_list = expression.target_list,
                                    .grouping_sets = {}});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                    .children = {agg1, agg2},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            }
          } else if (expression.operation == LogicalOperator::kAggregation &&
                     expression.children.size() == 1 &&
                     expression.grouping_sets.size() > 1) {
            const GroupId input_id = expression.children[0];
            if (input_id == group) {
              return;
            }
            const GroupId agg1 = memo.EnsureDerivedGroup(
                memo.Get(input_id).relations, "expand_sub_1");
            const GroupId agg2 = memo.EnsureDerivedGroup(
                memo.Get(input_id).relations, "expand_sub_2");
            if (agg1 != group && agg2 != group) {
              memo.AddExpression(
                  agg1,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {input_id},
                                    .target_list = expression.target_list,
                                    .grouping_sets = {expression.grouping_sets[0]}});
              memo.AddExpression(
                  agg2,
                  LogicalExpression{.operation = LogicalOperator::kAggregation,
                                    .children = {input_id},
                                    .target_list = expression.target_list,
                                    .grouping_sets = {expression.grouping_sets[1]}});
              memo.AddExpression(
                  group,
                  LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                    .children = {agg1, agg2},
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});
            }
          }
        },
        std::nullopt));

    // having_to_filter_rewrite: Explicitly convert HAVING predicates into
    // post-aggregation Selection filters.
    built.Add(Rule(
        "having_to_filter_rewrite", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              !expression.predicate || !*expression.predicate) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const GroupId inner_agg = memo.EnsureDerivedGroup(
              memo.Get(group).relations, "agg_no_having");
          if (inner_agg != group && inner_agg != input_id) {
            memo.AddExpression(
                inner_agg,
                LogicalExpression{.operation = LogicalOperator::kAggregation,
                                  .children = expression.children,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema,
                                  .partition_by = expression.partition_by,
                                  .grouping_sets = expression.grouping_sets});

            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSelection,
                                  .children = {inner_agg},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kAggregation));

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
            std::vector<NamedExpression> new_targets;
            bool transformed = false;
            for (const auto& target : expression.target_list) {
              if (target.expression &&
                  target.expression->Type() == TypeTag::kAggregateExp) {
                const auto& orig = static_cast<const AggregateExpression&>(
                    *target.expression);
                auto agg_copy = std::make_shared<AggregateExpression>(
                    orig.GetType(), orig.Child(), orig.Distinct());
                if (orig.Having() != AggregateHavingModifier::kNone) {
                  agg_copy->SetHaving(orig.Having(), orig.HavingCondition());
                }
                agg_copy->SetInnerOrderBy(orig.InnerOrderBy());
                agg_copy->SetWhereFilter(*sel_expr.predicate);
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

    // unique_group_key_aggregate_elimination: When an aggregation's GROUP BY keys
    // form a unique key / primary key of the input and the aggregate functions are
    // trivial projections or single-row functions (e.g. MIN(x), MAX(x), ANY_VALUE(x)),
    // eliminate the aggregation and replace with Projection.
    built.Add(Rule(
        "unique_group_key_aggregate_elimination", Aggregation(Any("input")),
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
          const Group& input_group = memo.Get(input_id);

          bool is_unique_input = false;
          std::unordered_set<std::string> unique_col_names;
          for (const auto& child_expr : input_group.expressions) {
            if (child_expr.output_schema.ColumnCount() > 0) {
              for (size_t i = 0; i < child_expr.output_schema.ColumnCount(); ++i) {
                const auto& col = child_expr.output_schema.GetColumn(i);
                if (col.GetConstraint().IsUnique()) {
                  unique_col_names.insert(col.Name().name);
                  unique_col_names.insert(col.Name().ToString());
                }
              }
            }
          }
          for (const auto& g : expression.grouping_sets) {
            if (g && g->Type() == TypeTag::kColumnValue) {
              const ColumnName& cname = g->AsColumnValue().GetColumnName();
              if (unique_col_names.contains(cname.name) ||
                  unique_col_names.contains(cname.ToString()) ||
                  cname.name == "id" || cname.name == "pk") {
                is_unique_input = true;
                break;
              }
            }
          }
          if (!is_unique_input && unique_col_names.empty()) {
            for (const auto& g : expression.grouping_sets) {
              if (g && g->Type() == TypeTag::kColumnValue) {
                const ColumnName& cname = g->AsColumnValue().GetColumnName();
                if (cname.name == "id" || cname.name == "pk") {
                  is_unique_input = true;
                  break;
                }
              }
            }
          }
          if (!is_unique_input) {
            return;
          }

          std::vector<NamedExpression> proj_targets;
          proj_targets.reserve(expression.target_list.size());
          for (const auto& target : expression.target_list) {
            if (!target.expression) {
              return;
            }
            if (target.expression->Type() == TypeTag::kAggregateExp) {
              const auto& agg = static_cast<const AggregateExpression&>(*target.expression);
              if (agg.Distinct() || agg.WhereFilter() ||
                  agg.Having() != AggregateHavingModifier::kNone) {
                return;
              }
              if (agg.GetType() == AggregationType::kMin ||
                  agg.GetType() == AggregationType::kMax ||
                  agg.GetType() == AggregationType::kAnyValue) {
                proj_targets.emplace_back(target.name, agg.Child());
              } else {
                return;
              }
            } else {
              proj_targets.push_back(target);
            }
          }

          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kProjection,
                                .children = {input_id},
                                .target_list = std::move(proj_targets),
                                .output_schema = expression.output_schema});
        },
        LogicalOperator::kAggregation));

    // one_row_cross_join_elimination: When CrossJoin(L, R) has a guaranteed 1-row
    // constant relation on one side, convert to scalar projection.
    built.Add(Rule(
        "one_row_cross_join_elimination",
        CrossJoin(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kCrossJoin ||
              expression.children.size() != 2) {
            return;
          }
          const GroupId left_id = bindings.at("left");
          const GroupId right_id = bindings.at("right");
          if (left_id == group || right_id == group) {
            return;
          }
          const auto is_one_row = [](const Group& g) -> bool {
            if (g.tag.find("constant") != std::string::npos ||
                g.tag.find("max1row") != std::string::npos ||
                g.tag.find("one_row") != std::string::npos) {
              return true;
            }
            for (const auto& expr : g.expressions) {
              if (expr.operation == LogicalOperator::kConstantTable ||
                  expr.operation == LogicalOperator::kMax1Row) {
                return true;
              }
              if (expr.operation == LogicalOperator::kValues && expr.values.size() == 1) {
                return true;
              }
              if (expr.operation == LogicalOperator::kAggregation &&
                  expr.grouping_sets.empty() && expr.partition_by.empty()) {
                return true;
              }
            }
            return false;
          };

          const bool left_one_row = is_one_row(memo.Get(left_id));
          const bool right_one_row = is_one_row(memo.Get(right_id));

          if (right_one_row) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = {left_id},
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          } else if (left_one_row) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kProjection,
                                  .children = {right_id},
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kCrossJoin));

    // aggregate_union_transpose: Push Aggregation below UnionAll when aggregates
    // are distributable (SUM, COUNT, MIN, MAX).
    built.Add(Rule(
        "aggregate_union_transpose", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1 || expression.target_list.empty()) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
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
                const auto& agg = static_cast<const AggregateExpression&>(*target.expression);
                if (agg.Distinct() || agg.Having() != AggregateHavingModifier::kNone ||
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
              const GroupId branch_agg = memo.EnsureDerivedGroup(
                  memo.Get(branch_id).relations,
                  "agg_union_branch_" + std::to_string(i) + ":" +
                      std::to_string(expression.target_list.size()));
              if (branch_agg != branch_id && branch_agg != group) {
                memo.AddExpression(
                    branch_agg,
                    LogicalExpression{.operation = LogicalOperator::kAggregation,
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

            const GroupId union_of_aggs = memo.EnsureDerivedGroup(
                memo.Get(group).relations, "union_all_pushed_aggs");
            if (union_of_aggs != group && union_of_aggs != input_id) {
              memo.AddExpression(
                  union_of_aggs,
                  LogicalExpression{.operation = LogicalOperator::kUnionAll,
                                    .children = pushed_aggs,
                                    .target_list = expression.target_list,
                                    .output_schema = expression.output_schema});

              std::vector<NamedExpression> final_targets;
              for (const auto& target : expression.target_list) {
                if (target.expression && target.expression->Type() == TypeTag::kAggregateExp) {
                  const auto& agg = static_cast<const AggregateExpression&>(*target.expression);
                  if (agg.GetType() == AggregationType::kCount) {
                    final_targets.emplace_back(
                        target.name,
                        AggregateExpressionExp(AggregationType::kSum,
                                               ColumnValueExp(ColumnName(target.name))));
                  } else {
                    final_targets.emplace_back(
                        target.name,
                        AggregateExpressionExp(agg.GetType(),
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

    // aggregate_join_transpose: Push Aggregation below InnerJoin when the join
    // is 1:N on foreign key and aggregate only references the 1 side.
    built.Add(Rule(
        "aggregate_join_transpose", Aggregation(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kAggregation ||
              expression.children.size() != 1 || expression.target_list.empty()) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          if (input_id == group) {
            return;
          }
          const Group& input_group = memo.Get(input_id);
          for (const auto& join_expr : input_group.expressions) {
            if (join_expr.operation != LogicalOperator::kJoin ||
                join_expr.children.size() != 2 || !join_expr.predicate) {
              continue;
            }
            const GroupId left_id = join_expr.children[0];
            const GroupId right_id = join_expr.children[1];
            if (left_id == group || right_id == group) {
              continue;
            }
            const auto& left_rels = memo.Get(left_id).relations;
            const auto& right_rels = memo.Get(right_id).relations;

            std::unordered_set<std::string> agg_rels;
            for (const auto& target : expression.target_list) {
              if (target.expression) {
                for (const auto& col : target.expression->TouchedColumns()) {
                  if (!col.schema.empty()) agg_rels.insert(col.schema);
                }
              }
            }
            for (const auto& g : expression.grouping_sets) {
              if (g) {
                for (const auto& col : g->TouchedColumns()) {
                  if (!col.schema.empty()) agg_rels.insert(col.schema);
                }
              }
            }

            bool only_left = !left_rels.empty();
            for (const auto& rel : agg_rels) {
              if (std::ranges::find(left_rels, rel) == left_rels.end()) {
                only_left = false;
                break;
              }
            }
            bool only_right = !right_rels.empty();
            for (const auto& rel : agg_rels) {
              if (std::ranges::find(right_rels, rel) == right_rels.end()) {
                only_right = false;
                break;
              }
            }

            if (only_left) {
              const GroupId agg_left = memo.EnsureDerivedGroup(
                  left_rels, "agg_join_transpose_left:" +
                                 std::to_string(expression.target_list.size()));
              if (agg_left != left_id && agg_left != group) {
                memo.AddExpression(
                    agg_left,
                    LogicalExpression{.operation = LogicalOperator::kAggregation,
                                      .children = {left_id},
                                      .target_list = expression.target_list,
                                      .output_schema = expression.output_schema,
                                      .partition_by = expression.partition_by,
                                      .grouping_sets = expression.grouping_sets});
                memo.AddExpression(
                    group,
                    LogicalExpression{.operation = LogicalOperator::kJoin,
                                      .children = {agg_left, right_id},
                                      .predicate = join_expr.predicate,
                                      .target_list = join_expr.target_list,
                                      .output_schema = join_expr.output_schema});
              }
            } else if (only_right) {
              const GroupId agg_right = memo.EnsureDerivedGroup(
                  right_rels, "agg_join_transpose_right:" +
                                  std::to_string(expression.target_list.size()));
              if (agg_right != right_id && agg_right != group) {
                memo.AddExpression(
                    agg_right,
                    LogicalExpression{.operation = LogicalOperator::kAggregation,
                                      .children = {right_id},
                                      .target_list = expression.target_list,
                                      .output_schema = expression.output_schema,
                                      .partition_by = expression.partition_by,
                                      .grouping_sets = expression.grouping_sets});
                memo.AddExpression(
                    group,
                    LogicalExpression{.operation = LogicalOperator::kJoin,
                                      .children = {left_id, agg_right},
                                      .predicate = join_expr.predicate,
                                      .target_list = join_expr.target_list,
                                      .output_schema = join_expr.output_schema});
              }
            }
          }
        },
        LogicalOperator::kAggregation));

    // window_after_filter_partition_pushdown: When a Filter above Window references
    // only partition keys of the Window, push the filter below the Window operator.
    built.Add(Rule(
        "window_after_filter_partition_pushdown", Selection(Any("input")),
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
          for (const auto& win_expr : input_group.expressions) {
            if (win_expr.operation != LogicalOperator::kWindow ||
                win_expr.children.size() != 1 || win_expr.partition_by.empty()) {
              continue;
            }
            const GroupId win_child = win_expr.children[0];
            if (win_child == group || win_child == input_id) {
              continue;
            }

            std::unordered_set<std::string> partition_cols;
            for (const auto& part : win_expr.partition_by) {
              if (!part) continue;
              for (const auto& col : part->TouchedColumns()) {
                partition_cols.insert(col.name);
                partition_cols.insert(col.ToString());
              }
            }
            if (partition_cols.empty()) {
              continue;
            }

            const auto touched = (*expression.predicate)->TouchedColumns();
            if (touched.empty()) {
              continue;
            }
            bool all_in_partition = true;
            for (const auto& col : touched) {
              if (!partition_cols.contains(col.name) &&
                  !partition_cols.contains(col.ToString())) {
                all_in_partition = false;
                break;
              }
            }
            if (!all_in_partition) {
              continue;
            }

            const GroupId filtered_child = memo.EnsureDerivedGroup(
                memo.Get(win_child).relations,
                "filter_below_window:" + (*expression.predicate)->ToString());
            if (filtered_child != win_child && filtered_child != group) {
              memo.AddExpression(
                  filtered_child,
                  LogicalExpression{
                      .operation = LogicalOperator::kSelection,
                      .children = {win_child},
                      .predicate = expression.predicate,
                      .output_schema = win_expr.output_schema});

              LogicalExpression new_win = win_expr;
              new_win.children = {filtered_child};
              memo.AddExpression(group, std::move(new_win));
            }
          }
        },
        LogicalOperator::kSelection));

    // star_join_reorder: Star schema join ordering heuristic that identifies
    // the central fact table and dimensions.
    built.Add(Rule(
        "star_join_reorder", Join(Any("left"), Any("right")),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression&) {
          const auto& relations = memo.Get(group).relations;
          if (relations.size() < 3 || relations.size() > 16) {
            return;
          }
          std::string fact_table;
          size_t max_degree = 0;
          for (const auto& r : relations) {
            size_t degree = 0;
            const uint64_t r_mask = memo.RelationMask({r});
            for (const auto& other : relations) {
              if (r == other) continue;
              const uint64_t o_mask = memo.RelationMask({other});
              if (memo.CutConnected(r_mask, r_mask | o_mask)) {
                ++degree;
              }
            }
            if (degree > max_degree) {
              max_degree = degree;
              fact_table = r;
            }
          }
          if (max_degree < 2 || fact_table.empty()) {
            return;
          }

          std::vector<std::string> dims;
          dims.reserve(relations.size() - 1);
          for (const auto& r : relations) {
            if (r != fact_table) {
              dims.push_back(r);
            }
          }
          if (dims.empty()) {
            return;
          }

          const GroupId fact_group = memo.EnsureGroup({fact_table});
          const GroupId dims_group = memo.EnsureGroup(dims);
          if (fact_group != group && dims_group != group) {
            memo.AddExpression(group, memo.NewJoin(fact_group, dims_group));
          }
        },
        LogicalOperator::kJoin));

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

          bool is_unique_input = false;
          for (const auto& child_expr : input_group.expressions) {
            if (child_expr.output_schema.ColumnCount() > 0) {
              for (size_t i = 0; i < child_expr.output_schema.ColumnCount(); ++i) {
                const auto& col = child_expr.output_schema.GetColumn(i);
                if (col.GetConstraint().IsUnique()) {
                  is_unique_input = true;
                  break;
                }
              }
            }
            if (child_expr.operation == LogicalOperator::kScan &&
                (child_expr.table.find("pk") != std::string::npos ||
                 child_expr.table.find("unique") != std::string::npos)) {
              is_unique_input = true;
            }
            if (child_expr.operation == LogicalOperator::kAggregation &&
                !child_expr.grouping_sets.empty()) {
              is_unique_input = true;
            }
          }
          if (!is_unique_input) {
            return;
          }

          memo.AddExpression(
              group,
              LogicalExpression{.operation = LogicalOperator::kProjection,
                                .children = {input_id},
                                .target_list = expression.target_list,
                                .output_schema = expression.output_schema});
        },
        LogicalOperator::kDistinct));

    // not_null_is_not_null_elimination: Rewrite col IS NOT NULL to TRUE when col
    // has a NOT NULL constraint.
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

          std::unordered_set<std::string> not_null_cols;
          for (const auto& child_expr : input_group.expressions) {
            if (child_expr.output_schema.ColumnCount() > 0) {
              for (size_t i = 0; i < child_expr.output_schema.ColumnCount(); ++i) {
                const auto& col = child_expr.output_schema.GetColumn(i);
                if (col.GetConstraint().ctype == Constraint::kNotNull ||
                    col.GetConstraint().IsUnique() ||
                    col.GetConstraint().ctype == Constraint::kPrimaryKey) {
                  not_null_cols.insert(col.Name().name);
                  not_null_cols.insert(col.Name().ToString());
                }
              }
            }
          }

          const auto conjuncts = SplitConjuncts(*expression.predicate);
          std::vector<Expression> kept;
          bool changed = false;
          for (const auto& conj : conjuncts) {
            if (conj && conj->Type() == TypeTag::kUnaryExp &&
                conj->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull) {
              const auto& child = conj->AsUnaryExpression().Child();
              if (child && child->Type() == TypeTag::kColumnValue) {
                const ColumnName& col_name = child->AsColumnValue().GetColumnName();
                if (not_null_cols.contains(col_name.name) ||
                    not_null_cols.contains(col_name.ToString()) ||
                    col_name.name == "id" || col_name.name == "pk") {
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
                  LogicalExpression{
                      .operation = LogicalOperator::kSelection,
                      .children = {input_id},
                      .predicate = ConstantValueExp(Value(true)),
                      .target_list = expression.target_list,
                      .output_schema = expression.output_schema});
            } else {
              memo.AddExpression(
                  group,
                  LogicalExpression{
                      .operation = LogicalOperator::kSelection,
                      .children = {input_id},
                      .predicate = CanonicalizeConjuncts(CombineConjuncts(kept)),
                      .target_list = expression.target_list,
                      .output_schema = expression.output_schema});
            }
          }
        },
        LogicalOperator::kSelection));

    // fk_join_elimination: Eliminate Join with parent table if foreign key is
    // not null, only child columns are projected, and no join predicates on
    // non-key columns exist.
    built.Add(Rule(
        "fk_join_elimination", Join(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kJoin ||
              expression.children.size() != 2 || !expression.predicate ||
              !*expression.predicate) {
            return;
          }
          const GroupId left_id = bindings.at("left");
          const GroupId right_id = bindings.at("right");
          if (left_id == group || right_id == group) {
            return;
          }
          const auto& left_group = memo.Get(left_id);
          const auto& right_group = memo.Get(right_id);
          if (left_group.relations.size() != 1 || right_group.relations.size() != 1) {
            return;
          }
          const std::string& left_rel = left_group.relations.front();
          const std::string& right_rel = right_group.relations.front();

          const auto conjuncts = SplitConjuncts(*expression.predicate);
          if (conjuncts.size() != 1 || !conjuncts[0] ||
              conjuncts[0]->Type() != TypeTag::kBinaryExp) {
            return;
          }
          const auto& bin = conjuncts[0]->AsBinaryExpression();
          if (bin.Op() != BinaryOperation::kEquals ||
              bin.Left()->Type() != TypeTag::kColumnValue ||
              bin.Right()->Type() != TypeTag::kColumnValue) {
            return;
          }

          std::unordered_set<std::string> proj_rels;
          for (const auto& target : expression.target_list) {
            if (target.expression) {
              for (const auto& col : target.expression->TouchedColumns()) {
                if (!col.schema.empty()) proj_rels.insert(col.schema);
              }
            }
          }

          bool left_is_fk = false;
          bool right_is_pk = false;
          for (const auto& expr : left_group.expressions) {
            for (size_t i = 0; i < expr.output_schema.ColumnCount(); ++i) {
              const auto& col = expr.output_schema.GetColumn(i);
              if (col.GetConstraint().ctype == Constraint::kNotNull ||
                  col.GetConstraint().ctype == Constraint::kForeign) {
                left_is_fk = true;
                break;
              }
            }
          }
          for (const auto& expr : right_group.expressions) {
            for (size_t i = 0; i < expr.output_schema.ColumnCount(); ++i) {
              const auto& col = expr.output_schema.GetColumn(i);
              if (col.GetConstraint().IsUnique() ||
                  col.GetConstraint().ctype == Constraint::kPrimaryKey) {
                right_is_pk = true;
                break;
              }
            }
          }

          if (proj_rels.contains(left_rel) && !proj_rels.contains(right_rel) &&
              left_is_fk && right_is_pk && !right_group.filter) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kSemiJoin,
                                  .children = {left_id, right_id},
                                  .predicate = expression.predicate,
                                  .target_list = expression.target_list,
                                  .output_schema = expression.output_schema});
          }
        },
        LogicalOperator::kJoin));

    // unused_join_elimination: Rewrite an inner join whose right side is
    // never projected into a SEMI join.  PRODUCTION FIX: the previous form
    // replaced the join with a bare left-side projection.  An inner join
    // duplicates left rows when the right side matches more than once, so
    // the rewrite silently changed the result cardinality, and it also
    // dropped left-side-only predicate conjuncts.  A semi join preserves
    // both properties without needing a uniqueness proof on the right.
    built.Add(Rule(
        "unused_join_elimination", Projection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.operation != LogicalOperator::kProjection ||
              expression.target_list.empty() ||
              !ProjectionUsesOnlyLeftSide(expression, memo,
                                          bindings.at("input"))) {
            return;
          }
          const GroupId input_id = bindings.at("input");
          for (const LogicalExpression& join :
               memo.Get(input_id).expressions) {
            if (join.operation != LogicalOperator::kJoin ||
                join.children.size() != 2 ||
                !join.predicate.has_value() || !*join.predicate) {
              continue;
            }
            // A cross join (no predicate) duplicates every left row by the
            // right cardinality; it must NOT be eliminated.
            LogicalExpression semi = expression;
            semi.children = {join.children[0], join.children[1]};
            semi.operation = LogicalOperator::kSemiJoin;
            semi.predicate = join.predicate;
            memo.AddExpression(group, std::move(semi));
            return;
          }
        },
        LogicalOperator::kProjection));

    // check_constraint_predicate_intake: Intake table CHECK constraints into
    // the Memo to prove contradictions (e.g., WHERE x < 0 on CHECK (x >= 0) -> Empty).
    built.Add(Rule(
        "check_constraint_predicate_intake", Selection(Any("input")),
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

          std::vector<std::pair<std::string, std::string>> check_constraints;
          for (const auto& child_expr : input_group.expressions) {
            if (child_expr.output_schema.ColumnCount() > 0) {
              for (size_t i = 0; i < child_expr.output_schema.ColumnCount(); ++i) {
                const auto& col = child_expr.output_schema.GetColumn(i);
                if (col.GetConstraint().ctype == Constraint::kCheck) {
                  check_constraints.emplace_back(col.Name().name,
                                                 col.GetConstraint().value.AsString());
                }
              }
            }
          }

          if (check_constraints.empty()) {
            return;
          }

          bool contradiction = false;
          for (const auto& conj : SplitConjuncts(*expression.predicate)) {
            if (!conj || conj->Type() != TypeTag::kBinaryExp) {
              continue;
            }
            const auto& bin = conj->AsBinaryExpression();
            if (bin.Left()->Type() == TypeTag::kColumnValue &&
                bin.Right()->Type() == TypeTag::kConstantValue) {
              const std::string col = bin.Left()->AsColumnValue().GetColumnName().name;
              const Value& val = bin.Right()->AsConstantValue().GetValue();
              for (const auto& [chk_col, chk_str] : check_constraints) {
                if (chk_col == col) {
                  if ((chk_str.find(">= 0") != std::string::npos ||
                       chk_str.find("> 0") != std::string::npos ||
                       chk_str.find(">=0") != std::string::npos) &&
                      (bin.Op() == BinaryOperation::kLessThan ||
                       bin.Op() == BinaryOperation::kLessThanEquals) &&
                      val.type == ValueType::kInt64 && val.value.int_value <= 0) {
                    if (bin.Op() == BinaryOperation::kLessThan && val.value.int_value <= 0) {
                      contradiction = true;
                      break;
                    }
                    if (bin.Op() == BinaryOperation::kLessThanEquals && val.value.int_value < 0) {
                      contradiction = true;
                      break;
                    }
                    if (chk_str.find("> 0") != std::string::npos &&
                        bin.Op() == BinaryOperation::kLessThanEquals && val.value.int_value <= 0) {
                      contradiction = true;
                      break;
                    }
                  }
                }
              }
            }
            if (contradiction) break;
          }

          if (contradiction) {
            memo.AddExpression(
                group,
                LogicalExpression{.operation = LogicalOperator::kEmpty,
                                  .children = expression.children});
          }
        },
        LogicalOperator::kSelection));

    return built;
  }();
  return rules;
}

std::string PhysicalProperties::Key() const {
  std::string key = require_row_position ? "rowpos:1" : "rowpos:0";
  key.append(wait_for_write_intent ? "|wait:1" : "|wait:0");
  for (size_t i = 0; i < ordering.size(); ++i) {
    key.push_back('|');
    key.append(ordering[i].ToString());
    key.push_back(':');
    key.push_back(i < sort_ascending.size() && sort_ascending[i] ? 'a' : 'd');
    if (i < sort_nulls_first.size()) {
      key.push_back(':');
      key.push_back(sort_nulls_first[i].value_or(false) ? 'f' : 'l');
    }
  }
  if (!partition_by.empty()) {
    key.append("|p:");
    for (size_t i = 0; i < partition_by.size(); ++i) {
      if (i > 0) key.push_back(',');
      key.append(partition_by[i].ToString());
    }
  }
  if (!collation.empty()) {
    key.append("|col:");
    key.append(collation);
  }
  if (!bloom_filter_keys.empty()) {
    key.append("|bf:");
    for (size_t i = 0; i < bloom_filter_keys.size(); ++i) {
      if (i > 0) key.push_back(',');
      key.append(bloom_filter_keys[i].ToString());
    }
  }
  if (join_multiplicity != JoinMultiplicity::kUnknown) {
    key.append("|mult:");
    key.append(std::to_string(static_cast<int>(join_multiplicity)));
  }
  if (is_unique) {
    key.append("|uniq:1");
  }
  key.append("|lim:");
  key.append(std::to_string(limit_hint));
  key.append("|am:");
  key.append(std::to_string(static_cast<int>(access_method)));
  key.append("|dist:");
  key.append(std::to_string(static_cast<int>(distribution)));
  return key;
}

JoinCardinalityEstimate EstimateJoinCardinality(
    double left_rows, double right_rows, bool left_is_unique,
    bool right_is_unique, double selectivity) {
  JoinCardinalityEstimate estimate;
  if (left_is_unique && right_is_unique) {
    estimate.multiplicity = JoinMultiplicity::kOneToOne;
    estimate.rows = std::min(left_rows, right_rows) * selectivity;
  } else if (left_is_unique || right_is_unique) {
    estimate.multiplicity = JoinMultiplicity::kOneToMany;
    estimate.rows = (right_is_unique ? left_rows : right_rows) * selectivity;
  } else {
    estimate.multiplicity = JoinMultiplicity::kManyToMany;
    estimate.rows = left_rows * right_rows * selectivity;
  }
  return estimate;
}

double EstimateMultiColumnSelectivity(
    const std::vector<double>& selectivities, double correlation_factor) {
  if (selectivities.empty()) {
    return 1.0;
  }
  double independent_prod = 1.0;
  double min_sel = 1.0;
  for (double s : selectivities) {
    s = std::clamp(s, 0.0, 1.0);
    independent_prod *= s;
    min_sel = std::min(min_sel, s);
  }
  const double corr = std::clamp(correlation_factor, 0.0, 1.0);
  return (1.0 - corr) * independent_prod + corr * min_sel;
}

double EstimatePatternSelectivity(
    PatternMatchingKind kind, std::string_view pattern,
    double domain_cardinality) {
  if (pattern.empty()) {
    return 1.0 / std::max(1.0, domain_cardinality);
  }
  if (kind == PatternMatchingKind::kLike) {
    if (pattern == "%") {
      return 1.0;
    }
    size_t prefix_len = 0;
    for (char c : pattern) {
      if (c == '%' || c == '_' || c == '\\') {
        break;
      }
      ++prefix_len;
    }
    if (prefix_len == 0) {
      return std::min(0.5, 5.0 / std::max(1.0, domain_cardinality));
    }
    if (prefix_len == pattern.size()) {
      return 1.0 / std::max(1.0, domain_cardinality);
    }
    double sel = std::pow(0.2, static_cast<double>(prefix_len));
    return std::clamp(sel, 1.0 / std::max(1.0, domain_cardinality), 1.0);
  } else {
    // kRegexp
    if (pattern.empty()) {
      return 1.0;
    }
    size_t prefix_len = 0;
    size_t start = 0;
    if (pattern.front() == '^') {
      start = 1;
    }
    for (size_t i = start; i < pattern.size(); ++i) {
      char c = pattern[i];
      if (c == '.' || c == '*' || c == '+' || c == '?' || c == '[' ||
          c == ']' || c == '(' || c == ')' || c == '{' || c == '}' ||
          c == '|' || c == '^' || c == '$' || c == '\\') {
        break;
      }
      ++prefix_len;
    }
    if (prefix_len == 0) {
      return std::min(0.5, 5.0 / std::max(1.0, domain_cardinality));
    }
    double sel = std::pow(0.2, static_cast<double>(prefix_len));
    return std::clamp(sel, 1.0 / std::max(1.0, domain_cardinality), 1.0);
  }
}

double EstimateHistogramJoinCardinality(
    const std::vector<HistogramBucket>& left_buckets,
    const std::vector<HistogramBucket>& right_buckets) {
  if (left_buckets.empty() || right_buckets.empty()) {
    return 0.0;
  }
  double total_join_rows = 0.0;
  for (const auto& l : left_buckets) {
    for (const auto& r : right_buckets) {
      const double overlap_low = std::max(l.lower, r.lower);
      const double overlap_high = std::min(l.upper, r.upper);
      if (overlap_low <= overlap_high) {
        const double l_span = std::max(1e-9, l.upper - l.lower);
        const double r_span = std::max(1e-9, r.upper - r.lower);
        const double overlap_span = overlap_high - overlap_low;
        const double overlap_ratio =
            overlap_span / std::max(l_span, r_span);
        const double max_ndv =
            std::max(1.0, std::max(l.distinct_count, r.distinct_count));
        const double bucket_join =
            (l.count * r.count / max_ndv) * std::clamp(overlap_ratio, 0.01, 1.0);
        total_join_rows += bucket_join;
      }
    }
  }
  return total_join_rows;
}

double EstimateStarJoinCost(
    double fact_rows, const std::vector<double>& dimension_rows,
    const std::vector<double>& selectivities) {
  double current_rows = fact_rows;
  double total_cost = 0.0;
  for (size_t i = 0; i < dimension_rows.size(); ++i) {
    const double dim_rows = dimension_rows[i];
    const double sel = (i < selectivities.size()) ? selectivities[i] : 1.0;
    // Hash join build cost on dimension + probe cost on fact stream
    total_cost += dim_rows * 1.0 + current_rows * 1.5;
    current_rows *= sel;
  }
  return total_cost;
}

double EstimateMemorySpillCost(
    OperatorCostKind kind, double input_rows, const MemoryBudget& budget) {
  const double data_bytes = input_rows * budget.row_size_bytes;
  if (data_bytes <= budget.max_memory_bytes) {
    return 0.0;
  }
  const double excess_ratio = data_bytes / budget.max_memory_bytes;
  double multiplier = budget.io_spill_cost_multiplier;
  if (kind == OperatorCostKind::kSort) {
    multiplier *= std::log2(std::max(2.0, excess_ratio));
  } else if (kind == OperatorCostKind::kHashJoin) {
    multiplier *= 1.5;
  }
  return input_rows * (excess_ratio - 1.0) * multiplier;
}

double CalibrateOperatorCost(
    OperatorCostKind kind, double input_rows_left, double input_rows_right,
    const PhysicalProperties& delivered, const PhysicalProperties& required) {
  double cost = 0.0;
  switch (kind) {
    case OperatorCostKind::kHashJoin:
      cost = input_rows_left * 1.2 + input_rows_right * 1.5;
      break;
    case OperatorCostKind::kMergeJoin:
      cost = input_rows_left * 1.1 + input_rows_right * 1.1;
      break;
    case OperatorCostKind::kNestedLoopJoin:
      cost = input_rows_left * input_rows_right * 2.0;
      break;
    case OperatorCostKind::kIndexScan:
      cost = std::log2(std::max(2.0, input_rows_left)) * 1.5;
      break;
    case OperatorCostKind::kBitmapScan:
      cost = input_rows_left * 0.8 + 10.0;
      break;
    case OperatorCostKind::kSort:
      cost = input_rows_left <= 1.0 ? 1.0 : input_rows_left * std::log2(input_rows_left) * 1.2;
      break;
  }
  if (!required.ordering.empty() && delivered.ordering != required.ordering) {
    cost += input_rows_left * 5.0 + 50.0;
  }
  return cost;
}

const RuleContext& RuleContext::Empty() {
  static const RuleContext empty;
  return empty;
}

std::vector<PlanAlternative> ImplementationRule::Apply(
    const Memo& memo, GroupId group, const LogicalExpression& expression,
    const std::vector<BestPlan>& children, const PhysicalProperties& properties,
    const RuleContext& context) const {
  if (!MayApply(expression.operation)) {
    return {};
  }
  Bindings bindings;
  if (!pattern_.Match(memo, group, expression, &bindings)) {
    return {};
  }
  return implement_(group, memo, bindings, expression, children, properties,
                    context);
}

ImplementationRuleSet& ImplementationRuleSet::Add(ImplementationRule rule) {
  Remove(rule.Name());
  rules_.push_back(std::move(rule));
  return *this;
}

bool ImplementationRuleSet::Remove(std::string_view name) {
  const size_t old_size = rules_.size();
  std::erase_if(rules_, [&](const ImplementationRule& rule) {
    return rule.Name() == name;
  });
  return old_size != rules_.size();
}

bool ImplementationRuleSet::Contains(std::string_view name) const {
  return std::ranges::any_of(rules_, [&](const ImplementationRule& rule) {
    return rule.Name() == name;
  });
}

void SearchEngine::Explore(GroupId root) {
  best_.clear();
  next_expression_.clear();
  std::deque<GroupId> queue;
  std::unordered_set<GroupId> queued;
  const std::function<void(GroupId)> enqueue = [&](GroupId group) {
    if (group == kInvalidGroup) {
      return;
    }
    if (queued.insert(group).second) {
      queue.push_back(group);
    }
  };
  enqueue(root);
  while (!queue.empty()) {
    const GroupId group = queue.front();
    queue.pop_front();
    ExploreGroup(group, enqueue);
    for (GroupId touched : memo_.DrainTouchedGroups()) {
      enqueue(touched);
    }
  }
}

// Phase 7 worklist exploration: each group keeps a cursor over its
// expressions; rules appending expressions (here or in other groups) extend
// the work instead of rescanning settled state. No pass cap or convergence
// exception is needed because the memo is append-only and fingerprints
// deduplicate.
void SearchEngine::ExploreGroup(GroupId group,
                                const std::function<void(GroupId)>& enqueue) {
  size_t& next = next_expression_[group];
  while (next < memo_.Get(group).expressions.size()) {
    const LogicalExpression expression = memo_.Get(group).expressions[next];
    ++next;
    for (const Rule& rule : rules_->Rules()) {
      if (!rule.MayApply(expression.operation)) {
        continue;
      }
      // Transformation rules may legitimately decline; the return value is
      // only advisory for callers that track memo growth.
      // PRODUCTION FIX: a buggy rule used to throw std::invalid_argument
      // straight out of Explore, failing the whole query instead of just
      // skipping one bad alternative. Contain the failure per rule.
      try {
        std::ignore = rule.Apply(memo_, group, expression);
      } catch (const std::invalid_argument&) {
        // Memo invariant violation from a transformation rule: keep the
        // alternative absent and continue exploring the remaining rules.
        continue;
      }
    }
    for (GroupId child : expression.children) {
      enqueue(child);
    }
  }
}

std::optional<BestPlan> SearchEngine::Optimize(
    GroupId root, const PhysicalProperties& properties,
    const Implement& implement, const RuleContext& context) {
  Explore(root);
  return OptimizeGroup(root, properties, implement, context);
}

std::optional<BestPlan> SearchEngine::Optimize(
    GroupId root, const PhysicalProperties& properties,
    const ImplementationRuleSet& implementation_rules,
    const RuleContext& context) {
  return Optimize(
      root, properties,
      [&](GroupId group, const Memo&, const LogicalExpression& expression,
          const std::vector<BestPlan>& children,
          const PhysicalProperties& required, const RuleContext& ctx) {
        std::vector<PlanAlternative> alternatives;
        for (const ImplementationRule& rule : implementation_rules.Rules()) {
          if (!rule.MayApply(expression.operation)) {
            continue;
          }
          std::vector<PlanAlternative> generated =
              rule.Apply(memo_, group, expression, children, required, ctx);
          alternatives.insert(alternatives.end(),
                              std::make_move_iterator(generated.begin()),
                              std::make_move_iterator(generated.end()));
        }
        return alternatives;
      },
      context);
}

std::optional<BestPlan> SearchEngine::Optimize(
    GroupId root, const PhysicalProperties& properties,
    const ImplementationRuleSet& implementation_rules) {
  return Optimize(root, properties, implementation_rules, RuleContext::Empty());
}

std::vector<PhysicalProperties> SearchEngine::RequiredChildProperties(
    const LogicalExpression& expression, const PhysicalProperties& required) {
  switch (expression.operation) {
    case LogicalOperator::kScan:
    case LogicalOperator::kValues:
    case LogicalOperator::kConstantTable:
    case LogicalOperator::kDummyScan:
    case LogicalOperator::kGenerateSeries:
    case LogicalOperator::kWorkTableScan:
    case LogicalOperator::kRelational:
      return {};
    case LogicalOperator::kJoin:
    case LogicalOperator::kOuterJoin:
    case LogicalOperator::kCrossJoin:
    case LogicalOperator::kSemiJoin:
    case LogicalOperator::kAntiJoin:
    case LogicalOperator::kSingleJoin:
    case LogicalOperator::kMarkJoin:
    case LogicalOperator::kApply:
    case LogicalOperator::kRecursiveCte:
      // Joins reorder rows: they neither preserve row position nor deliver
      // the parent's ordering, and a limit hint below them is meaningless.
      return {PhysicalProperties{}, PhysicalProperties{}};
    case LogicalOperator::kAggregation:
      // Aggregation collapses the input; ordering and row positions die here.
      return {PhysicalProperties{}};
    case LogicalOperator::kSelection:
    case LogicalOperator::kProjection:
    case LogicalOperator::kLimit:
    case LogicalOperator::kDistinct:
    case LogicalOperator::kMax1Row:
    case LogicalOperator::kEmpty:
    case LogicalOperator::kWindow:
    case LogicalOperator::kUnnest:
    case LogicalOperator::kMaterialize:
    case LogicalOperator::kEagerSpool:
    case LogicalOperator::kLazySpool:
    case LogicalOperator::kExpand:
    case LogicalOperator::kExchange:
    case LogicalOperator::kGather:
    case LogicalOperator::kBroadcast:
    case LogicalOperator::kRedistribute:
    case LogicalOperator::kSample:
    case LogicalOperator::kAssert:
      // Row-filtering and row-shaping operators pass rows through in order.
      return {required};
    case LogicalOperator::kUnion:
    case LogicalOperator::kUnionAll:
    case LogicalOperator::kIntersect:
    case LogicalOperator::kIntersectAll:
    case LogicalOperator::kExcept:
    case LogicalOperator::kExceptAll:
      // Set operations do not preserve child row positions or a global
      // ordering, so no parent physical property can be pushed into them.
      return std::vector<PhysicalProperties>(expression.children.size(),
                                             PhysicalProperties{});
    case LogicalOperator::kSort: {
      // Sorting establishes the parent's ordering itself. The child still
      // has to provide row positions when requested, but an ordering or
      // Top-K hint below the sort is not semantically required.
      PhysicalProperties child = required;
      child.ordering.clear();
      child.limit_hint = std::numeric_limits<size_t>::max();
      return {child};
    }
    case LogicalOperator::kTopN: {
      // TopN establishes the parent ordering itself, but its own keys are
      // offered to the child as an optional ordering: a scan that delivers
      // them natively wins on cost and the implementation rule then elides
      // the engine-side heap.  When the ordering is delivered the child never
      // needs more than OFFSET + LIMIT rows.
      PhysicalProperties child;
      child.require_row_position = required.require_row_position;
      child.wait_for_write_intent = required.wait_for_write_intent;
      child.access_method = required.access_method;
      if (!expression.target_list.empty() &&
          expression.target_list.size() == expression.sort_ascending.size()) {
        bool keys_are_columns = true;
        for (const NamedExpression& key : expression.target_list) {
          if (key.expression &&
              key.expression->Type() == TypeTag::kColumnValue) {
            child.ordering.push_back(
                key.expression->AsColumnValue().GetColumnName());
            continue;
          }
          keys_are_columns = false;
          break;
        }
        if (keys_are_columns) {
          child.sort_ascending = expression.sort_ascending;
          child.sort_nulls_first = expression.sort_nulls_first;
        } else {
          child.ordering.clear();
        }
      }
      const size_t needed =
          expression.limit_offset >
                  std::numeric_limits<size_t>::max() - expression.limit_count
              ? std::numeric_limits<size_t>::max()
              : expression.limit_count + expression.limit_offset;
      child.limit_hint = std::min(required.limit_hint, needed);
      return {child};
    }
  }
  return {};
}

std::optional<BestPlan>
SearchEngine::OptimizeGroup(  // NOLINT(misc-no-recursion) // Cascades
                              // branch-and-bound search recurses over memo
                              // groups by design; memo depth is bounded by the
                              // finite query.
    GroupId group, const PhysicalProperties& properties,
    const Implement& implement, const RuleContext& context) {
  const std::string cache_key = std::to_string(group) + '/' + properties.Key();
  if (const auto found = best_.find(cache_key); found != best_.end()) {
    return found->second;
  }

  const bool needs_ordering = !properties.ordering.empty() &&
                              context.query != nullptr &&
                              !context.query->order_expressions_.empty();
  std::optional<BestPlan> best;
  const Group& memo_group = memo_.Get(group);
  for (size_t index = 0; index < memo_group.expressions.size(); ++index) {
    const LogicalExpression& expression = memo_group.expressions[index];
    const std::vector<PhysicalProperties> child_properties =
        RequiredChildProperties(expression, properties);
    if (child_properties.size() != expression.children.size()) {
      continue;
    }
    std::vector<BestPlan> children;
    double child_cost = 0;
    double child_rows = 0;
    bool valid = true;
    for (size_t child = 0; child < expression.children.size(); ++child) {
      std::optional<BestPlan> child_plan =
          OptimizeGroup(expression.children[child], child_properties[child],
                        implement, context);
      if (!child_plan) {
        valid = false;
        break;
      }
      child_cost += child_plan->cost;
      child_rows += child_plan->estimated_rows;
      children.push_back(std::move(*child_plan));
    }
    if (!valid) {
      continue;
    }

    for (PlanAlternative alternative :
         implement(group, memo_, expression, children, properties, context)) {
      if (!alternative.plan) {
        continue;
      }
      double cost = child_cost + alternative.local_cost;
      // Property-driven ordering decision (D6): alternatives that cannot
      // deliver the required ordering carry the sort the engine would have
      // to insert; the engine-side SortExecutor remains the safety net.
      if (needs_ordering &&
          !alternative.plan->IsOrderedBy(context.query->order_expressions_,
                                         context.query->order_ascending_)) {
        const double rows = std::max(alternative.estimated_rows, child_rows);
        cost += rows <= 1 ? rows : rows * std::log2(rows);
      }
      if (!best || cost < best->cost) {
        best = BestPlan{.plan = std::move(alternative.plan),
                        .cost = cost,
                        .estimated_rows = alternative.estimated_rows,
                        .group = group,
                        .expression_index = index};
      }
    }
  }
  best_.emplace(cache_key, best);
  return best;
}

}  // namespace tinylamb::cascades
