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

#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
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
  if (!predicate) { return nullptr;
}
  std::vector<Expression> conjuncts = SplitConjuncts(predicate);
  std::ranges::sort(conjuncts, [](const Expression& a, const Expression& b) {
    return a->ToString() < b->ToString();
  });
  conjuncts.erase(std::ranges::unique(
                      conjuncts,
                      [](const Expression& a, const Expression& b) {
                        return a->ToString() == b->ToString();
                      })
                      .begin(),
                  conjuncts.end());
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
      if (!column.schema.empty()) { touched.insert(column.schema);
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
  if (!expression) { return false;
}
  if (expression->Type() == TypeTag::kAggregateExp) { return true;
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
  if (!expression) { return expression;
}
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    for (const NamedExpression& output : outputs) {
      if (OutputMatchesColumn(output, column)) { return output.expression;
}
    }
    return std::nullopt;
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  if (children.empty()) { return expression;
}
  std::vector<Expression> rewritten;
  rewritten.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped = RewriteThroughOutputs(child, outputs);
    if (!mapped) { return std::nullopt;
}
    rewritten.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expression, std::move(rewritten));
}

std::vector<NamedExpression> GroupingOutputs(
    const std::vector<NamedExpression>& outputs) {
  std::vector<NamedExpression> grouping;
  for (const NamedExpression& output : outputs) {
    if (!ContainsAggregate(output.expression)) { grouping.push_back(output);
}
  }
  return grouping;
}

std::optional<Value> EqualityConstant(const Expression& predicate,
                                      const ColumnName& column) {
  if (!predicate) { return std::nullopt;
}
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) { continue;
}
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Op() != BinaryOperation::kEquals) { continue;
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
  if (!join_predicate) { return;
}
  for (const Expression& conjunct : SplitConjuncts(join_predicate)) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) { continue;
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
      if (from.schema.empty() || to.schema.empty()) { return;
}
      const std::optional<Value> constant =
          EqualityConstant(memo.Get(memo.EnsureGroup({from.schema})).filter,
                           from);
      if (!constant) { return;
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

}  // namespace

std::string LogicalExpression::Fingerprint() const {
  std::ostringstream out;
  out << static_cast<int>(operation) << ':' << table;
  for (GroupId child : children) { out << ':' << child;
}
  if (operation == LogicalOperator::kOuterJoin) {
    out << "#j:" << static_cast<unsigned>(join_type);
  }
  if (predicate) {
    out << "#p:" << (*predicate)->ToString();
  }
  for (const NamedExpression& item : target_list) {
    out << "#t:" << item.name << '=' << item.expression->ToString();
  }
  if (operation == LogicalOperator::kSort ||
      operation == LogicalOperator::kTopN) {
    out << "#s:";
    for (size_t i = 0; i < target_list.size(); ++i) {
      out << (i < sort_ascending.size() && sort_ascending[i] ? 'a' : 'd');
      if (i < sort_nulls_first.size() && sort_nulls_first[i].has_value()) {
        out << (*sort_nulls_first[i] ? 'f' : 'l');
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
    out << "#relational:" << static_cast<const void*>(relational_statement.get());
  }
  if (operation == LogicalOperator::kValues) {
    out << "#v:" << output_schema.ColumnCount() << ':' << values.size();
    for (const Row& row : values) { out << ':' << row; }
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
  if (relations.empty()) { throw std::invalid_argument("empty join graph");
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
  if (matching.empty()) { return nullptr;
}
  return CanonicalizeConjuncts(CombineConjuncts(matching));
}

Expression Memo::JoinConditionFor(const Group& left, const Group& right) const {
  const uint64_t union_mask = left.relation_mask | right.relation_mask;
  std::vector<Expression> spanning;
  for (size_t i = 0; i < conjuncts_.size(); ++i) {
    const uint64_t mask = conjunct_masks_[i];
    if ((mask & ~union_mask) != 0) { continue;
}
    if (mask == 0) { continue;
}
    if ((mask & ~left.relation_mask) == 0) { continue;
}
    if ((mask & ~right.relation_mask) == 0) { continue;
}
    spanning.push_back(conjuncts_[i]);
  }
  if (spanning.empty()) { return nullptr;
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
    if (!memo.CutConnected(pivot_mask, within)) { continue;
}
    std::vector<std::string> left{pivot};
    std::vector<std::string> right;
    for (const std::string& relation : relations) {
      if (relation != pivot) { right.push_back(relation);
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
  if (condition) { join.predicate = condition;
}
  return join;
}

void Memo::MergeScanFilter(GroupId group, const Expression& predicate) {
  Group& target = Get(group);
  if (target.relations.size() != 1) {
    throw std::invalid_argument("scan filter requires a single-relation group");
  }
  const Expression next = CanonicalizeConjuncts(
      target.filter ? BinaryExpressionExp(target.filter,
                                          BinaryOperation::kAnd, predicate)
                    : predicate);
  target.filter = next;
}

bool Memo::CutConnected(uint64_t left_mask, uint64_t within_mask) const {
  const uint64_t right_mask = within_mask & ~left_mask;
  if (left_mask == 0 || right_mask == 0) { return false;
}
  return std::ranges::any_of(conjunct_masks_, [left_mask, right_mask](
                                                  uint64_t mask) {
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

GroupId Memo::EnsureGroup(std::vector<std::string> relations) {  // NOLINT(misc-no-recursion) // Cascades memo construction recurses over join operands by design; relation count is bounded (kMaxJoinEnumerationRelations).
  relations = Normalize(std::move(relations));
  if (relations.empty()) { throw std::invalid_argument("empty memo group");
}
  const std::string key = GroupKey(relations);
  if (const auto found = groups_by_key_.find(key);
      found != groups_by_key_.end()) {
    return found->second;
  }

  const GroupId id = groups_.size();
  groups_by_key_.emplace(key, id);
  const uint64_t mask = RelationMask(relations);
  groups_.push_back(Group{.id=id, .relations=relations, .expressions={}, .filter=nullptr, .relation_mask=mask, .tag=""});
  if (relations.size() == 1) {
    groups_.back().filter = ScanFilterFor(groups_.back());
    AddExpression(
        id, LogicalExpression{.operation = LogicalOperator::kScan,
                              .table = relations.front()});
    return id;
  }

  auto [left, right] = ConnectedSplit(*this, relations);
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
  groups_.push_back(
      Group{.id=id, .relations=normalized, .expressions={}, .filter=nullptr, .relation_mask=RelationMask(normalized),
            .tag=std::string(tag)});
  return id;
}

bool Memo::AddExpression(GroupId group, LogicalExpression expression) {
  Group& target = Get(group);
  switch (expression.operation) {
    case LogicalOperator::kScan:
      if (target.relations.size() != 1 ||
          target.relations.front() != expression.table ||
          !expression.children.empty() || expression.predicate) {
        throw std::invalid_argument("scan does not belong to memo group");
      }
      break;
    case LogicalOperator::kJoin:
    case LogicalOperator::kOuterJoin:
    case LogicalOperator::kCrossJoin:
    case LogicalOperator::kSemiJoin:
    case LogicalOperator::kAntiJoin: {
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
        throw std::invalid_argument("join children are not equivalent to group");
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
        throw std::invalid_argument("set operation needs at least two children");
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
    case LogicalOperator::kEmpty: {
      if (expression.children.size() != 1) {
        throw std::invalid_argument("single-child logical operator");
      }
      if (expression.children[0] == group) {
        throw std::invalid_argument("logical operator references its own group");
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
    case LogicalOperator::kRelational:
      if (!expression.children.empty() || !expression.relational_statement) {
        throw std::invalid_argument(
            "relational IR must carry a statement and have no children");
      }
      break;
    case LogicalOperator::kValues:
    case LogicalOperator::kDummyScan:
      if (!expression.children.empty()) {
        throw std::invalid_argument("constant source cannot have children");
      }
      if (expression.operation == LogicalOperator::kValues &&
          expression.output_schema.ColumnCount() == 0 &&
          !expression.values.empty()) {
        throw std::invalid_argument("VALUES needs an output schema");
      }
      break;
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
  if (group >= groups_.size()) { throw std::out_of_range("memo group");
}
  return groups_[group];
}

Group& Memo::Get(GroupId group) {
  if (group >= groups_.size()) { throw std::out_of_range("memo group");
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
      if (i > 0) { out << ",";
}
      out << group.relations[i];
    }
    out << "}";
    if (!group.tag.empty()) { out << " tag=" << group.tag;
}
    if (group.filter) { out << " filter=" << *group.filter;
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
  Pattern pattern =
      Op(operation, std::move(children), std::move(capture));
  pattern.payload_ = payload;
  return pattern;
}

bool Pattern::MatchGroup(const Memo& memo, GroupId group,  // NOLINT(misc-no-recursion) // Cascades pattern matching recurses over the expression tree by design; depth is bounded by the memo built from a finite query.
                         Bindings* bindings) const {
  if (!capture_.empty()) {
    auto [iter, inserted] = bindings->emplace(capture_, group);
    if (!inserted && iter->second != group) { return false;
}
  }
  if (!operation_) { return true;
}
  return std::ranges::any_of(
      memo.Get(group).expressions,
      [&](const LogicalExpression& expression) {  // NOLINT(misc-no-recursion) // Part of Pattern::MatchGroup recursion; bounded by the finite memo.
        Bindings local = *bindings;
        if (Match(memo, group, expression, &local)) {
          *bindings = std::move(local);
          return true;
        }
        return false;
      });
}

bool Pattern::MatchPayload(const Memo& memo, const LogicalExpression& expression,
                           [[maybe_unused]] const Bindings& bindings) const {
  if (payload_.requires_predicate && !expression.predicate) { return false;
}
  if (!payload_.predicate_within_child) { return true;
}
  const size_t child_index = *payload_.predicate_within_child;
  if (!expression.predicate || child_index >= expression.children.size()) {
    return false;
  }
  const Group& child = memo.Get(expression.children[child_index]);
  return std::ranges::all_of(
      (*expression.predicate)->TouchedColumns(),
      [&child](const ColumnName& column) {
        // Unqualified names cannot be proven to belong to the child, so they
        // fail the constraint (strict interpretation).
        return !column.schema.empty() &&
               std::ranges::find(child.relations, column.schema) !=
                   child.relations.end();
      });
}

bool Pattern::Match(const Memo& memo, GroupId group,  // NOLINT(misc-no-recursion) // Cascades pattern matching recurses over the expression tree by design; depth is bounded by the memo built from a finite query.
                    const LogicalExpression& expression,
                    Bindings* bindings) const {
  if (operation_ && expression.operation != *operation_) { return false;
}
  if (!children_.empty() && children_.size() != expression.children.size()) {
    return false;
  }
  Bindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, group);
    if (!inserted && iter->second != group) { return false;
}
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].MatchGroup(memo, expression.children[i], &local)) {
      return false;
    }
  }
  if (!MatchPayload(memo, expression, local)) { return false;
}
  *bindings = std::move(local);
  return true;
}

bool Rule::Apply(Memo& memo, GroupId group,
                 const LogicalExpression& expression) const {
  if (!MayApply(expression.operation)) { return false;
}
  Bindings bindings;
  if (!pattern_.Match(memo, group, expression, &bindings)) { return false;
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
  for (const Rule& rule : rules_) { names.push_back(rule.Name());
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
          memo.AddExpression(
              group, memo.NewJoin(expression.children[1], expression.children[0]));
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "join_enumeration", Join(),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression&) {
          const std::vector<std::string> relations = memo.Get(group).relations;
          if (relations.size() < 3) { return;
}
          // Fallback for very large joins: keep the initial connected-split
          // join order instead of walking 2^n bipartitions.
          if (relations.size() > kMaxJoinEnumerationRelations) { return;
}
          const uint64_t within = memo.Get(group).relation_mask;
          // Phase 7: with a connected join graph, bipartitions whose cut is
          // not crossed by any conjunct are pure cross products and can be
          // pruned; a fully disconnected graph keeps exhaustive enumeration.
          const bool prune = !memo.JoinGraphDisconnected();
          const uint64_t limit = uint64_t{1} << relations.size();
          for (uint64_t mask = 1; mask + 1 < limit; ++mask) {
            if ((mask & 1U) == 0) { continue;
}
            if (prune && !memo.CutConnected(mask, within)) { continue;
}
            std::vector<std::string> left;
            std::vector<std::string> right;
            for (size_t i = 0; i < relations.size(); ++i) {
              ((mask >> i) & 1U ? left : right).push_back(relations[i]);
            }
            if (left.empty() || right.empty()) { continue;
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
        Join(Pattern::Op(LogicalOperator::kJoin, {Any("ll"), Any("lr")}, "left"),
             Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression&) {
          const GroupId inner = memo.EnsureGroup(UnionRelations(
              memo.Get(bindings.at("lr")).relations,
              memo.Get(bindings.at("right")).relations));
          memo.AddExpression(group,
                             memo.NewJoin(bindings.at("ll"), inner));
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "join_associativity_right",
        Join(Any("left"),
             Pattern::Op(LogicalOperator::kJoin, {Any("rl"), Any("rr")},
                         "right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression&) {
          const GroupId inner = memo.EnsureGroup(UnionRelations(
              memo.Get(bindings.at("left")).relations,
              memo.Get(bindings.at("rl")).relations));
          memo.AddExpression(group,
                             memo.NewJoin(inner, bindings.at("rr")));
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "join_to_cross_if_no_predicate",
        Join(Any("left"), Any("right")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (expression.predicate) { return; }
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kCrossJoin,
                                       .children = {bindings.at("left"),
                                                    bindings.at("right")}});
        },
        LogicalOperator::kJoin));
    built.Add(Rule(
        "eliminate_false_selection",
        Selection(Any()),
        [](const Bindings&, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate ||
              (*expression.predicate)->Type() != TypeTag::kConstantValue) {
            return;
          }
          const Value value =
              (*expression.predicate)->AsConstantValue().GetValue();
          if (!value.IsNull() && value.Truthy()) { return; }
          memo.AddExpression(
              group, LogicalExpression{.operation = LogicalOperator::kEmpty,
                                       .children = expression.children});
        },
        LogicalOperator::kSelection));
    // Selection(Selection(X, p1), p2) -> Selection(X, p1 AND p2): adds the
    // merged expression (same child group) with canonical conjunct order.
    built.Add(Rule(
        "merge_selections",
        Selection(Selection(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          const Group& inner_group = memo.Get(bindings.at("inner"));
          for (const LogicalExpression& inner : inner_group.expressions) {
            if (inner.operation != LogicalOperator::kSelection) { continue;
}
            const Expression merged = CanonicalizeConjuncts(BinaryExpressionExp(
                *inner.predicate, BinaryOperation::kAnd,
                *expression.predicate));
            memo.AddExpression(group,
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
        "push_selection_into_scan",
        SelectionWithin(0, Scan("scan")),
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
        "push_selection_through_join",
        Selection(Join(Any(), Any(), "input")),
        [](const Bindings& bindings, Memo& memo, GroupId,
           const LogicalExpression& expression) {
          std::unordered_set<std::string> left_relations;
          for (const LogicalExpression& join :
               memo.Get(bindings.at("input")).expressions) {
            if (join.operation != LogicalOperator::kJoin) { continue;
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
        "split_selection_over_join",
        Selection(Join(Any(), Any(), "input")),
        [](const Bindings&, Memo& memo, GroupId,
           const LogicalExpression& expression) {
          PushSingleRelationConjuncts(
              memo, *expression.predicate,
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
            if (!is_setop || !expression.predicate) { continue; }
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
                "filter-before-distinct:" + (*expression.predicate)->ToString());
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
              if (!item) { return; }
              for (const ColumnName& column : item->TouchedColumns()) {
                if (column.schema.empty()) {
                  safe = false;
                  return;
                }
                const bool in_left = std::ranges::find(
                                         left.relations, column.schema) !=
                                     left.relations.end();
                const bool in_right = std::ranges::find(
                                          right.relations, column.schema) !=
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
              if (!safe) { break; }
            }
            if (safe && join.predicate) { collect(*join.predicate); }
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
        "merge_projections",
        Projection(Projection(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& inner :
               memo.Get(bindings.at("inner")).expressions) {
            if (inner.operation != LogicalOperator::kProjection) { continue;
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
            if (!ok || composed.empty()) { continue;
}
            memo.AddExpression(group,
                               LogicalExpression{
                                   .operation = LogicalOperator::kProjection,
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
            if (!rewritten) { continue;
}
            const GroupId input = projection.children[0];
            const GroupId filtered = memo.EnsureDerivedGroup(
                memo.Get(input).relations,
                "sel-below-proj:" + (*rewritten)->ToString());
            memo.AddExpression(
                filtered, LogicalExpression{.operation = LogicalOperator::kSelection,
                                            .children = {input},
                                            .predicate = *rewritten});
            memo.AddExpression(
                group, LogicalExpression{.operation = LogicalOperator::kProjection,
                                         .children = {filtered},
                                         .target_list = projection.target_list});
          }
        },
        LogicalOperator::kSelection));
    // Limit(Projection(X)): project after the cut so the scan/join below
    // produces fewer rows (ProjectLimitTranspose / LimitProjectTranspose).
    built.Add(Rule(
        "push_limit_through_projection",
        Limit(Projection(Any(), "proj")),
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
                group, LogicalExpression{.operation = LogicalOperator::kProjection,
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
                  RewriteThroughOutputs(key.expression,
                                        projection.target_list);
              if (!translated) {
                translatable = false;
                break;
              }
              translated_keys.emplace_back("", *translated);
            }
            if (!translatable || translated_keys.empty()) { continue; }

            const std::vector<std::string> input_relations =
                memo.Get(projection.children[0]).relations;
            std::string signature = "topn-below-proj:" +
                                    std::to_string(expression.limit_count) +
                                    ":" +
                                    std::to_string(expression.limit_offset);
            for (const NamedExpression& key : translated_keys) {
              signature.push_back('|');
              signature.append(key.expression->ToString());
            }
            const GroupId pushed =
                memo.EnsureDerivedGroup(input_relations, signature);
            memo.AddExpression(
                pushed,
                LogicalExpression{.operation = LogicalOperator::kTopN,
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
                  std::numeric_limits<size_t>::max() -
                      expression.limit_count) {
            return;
          }
          if (memo.Get(bindings.at("input")).tag.starts_with(
                  "union-limit-setop:")) {
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
              const GroupId limited = memo.EnsureDerivedGroup(
                  memo.Get(child).relations,
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
            if (pushed.empty()) { continue;
}
            const GroupId input = aggregation.children[0];
            const GroupId filtered = memo.EnsureDerivedGroup(
                memo.Get(input).relations,
                "sel-below-agg:" + CombineConjuncts(pushed)->ToString());
            memo.AddExpression(
                filtered, LogicalExpression{.operation = LogicalOperator::kSelection,
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
                group, LogicalExpression{.operation = LogicalOperator::kSelection,
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
        "merge_limits",
        Limit(Limit(Any(), "inner")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          for (const LogicalExpression& inner :
               memo.Get(bindings.at("inner")).expressions) {
            if (inner.operation != LogicalOperator::kLimit) { continue;
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
              const std::optional<bool> a_null =
                  ai < a.sort_nulls_first.size() ? a.sort_nulls_first[ai]
                                                 : std::nullopt;
              const std::optional<bool> b_null =
                  bi < b.sort_nulls_first.size() ? b.sort_nulls_first[bi]
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
                if (!same_key(prefix, i, full, i)) { return false; }
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
        "eliminate_true_selection",
        Selection(Any("input")),
        [](const Bindings& bindings, Memo& memo, GroupId group,
           const LogicalExpression& expression) {
          if (!expression.predicate || !*expression.predicate ||
              (*expression.predicate)->Type() != TypeTag::kConstantValue) {
            return;
          }
          const Value value =
              (*expression.predicate)->AsConstantValue().GetValue();
          if (value.IsNull() || !value.Truthy()) { return;
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
          const bool except = expression.operation == LogicalOperator::kExcept ||
                              expression.operation == LogicalOperator::kExceptAll;
          if ((!intersect && !except) || expression.children.size() < 2) {
            return;
          }
          const bool empty_branch = std::ranges::any_of(
              expression.children, [&](GroupId child) {
                return std::ranges::any_of(
                    memo.Get(child).expressions,
                    [](const LogicalExpression& candidate) {
                      return candidate.operation == LogicalOperator::kEmpty;
                    });
              });
          const bool empty_left =
              except && std::ranges::any_of(
                            memo.Get(expression.children.front()).expressions,
                            [](const LogicalExpression& candidate) {
                              return candidate.operation == LogicalOperator::kEmpty;
                            });
          if (!empty_branch && !empty_left) { return; }
          const GroupId base = memo.EnsureGroup(memo.Get(group).relations);
          if (base == group) { return; }
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
            return std::ranges::any_of(
                memo.Get(child).expressions,
                [](const LogicalExpression& candidate) {
                  return candidate.operation == LogicalOperator::kEmpty;
                });
          };
          const auto copy_child = [&](GroupId child) {
            for (const LogicalExpression& alternative :
                 memo.Get(child).expressions) {
              memo.AddExpression(group, alternative);
            }
          };
          if (expression.children.size() < 2) { return; }
          const bool has_empty = std::ranges::any_of(expression.children, is_empty);
          if (!has_empty) { return; }

          if (expression.operation == LogicalOperator::kUnionAll) {
            std::vector<GroupId> survivors;
            for (GroupId child : expression.children) {
              if (!is_empty(child)) { survivors.push_back(child); }
            }
            std::vector<std::string> survivor_relations;
            for (GroupId child : survivors) {
              survivor_relations =
                  UnionRelations(survivor_relations, memo.Get(child).relations);
            }
            if (survivor_relations != memo.Get(group).relations) { return; }
            if (survivors.size() == 1) {
              copy_child(survivors.front());
            } else if (survivors.size() >= 2) {
              memo.AddExpression(
                  group, LogicalExpression{.operation = LogicalOperator::kUnionAll,
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
            if (!is_empty(expression.children.back())) { return; }
            if (memo.Get(expression.children.front()).relations !=
                memo.Get(group).relations) {
              return;
            }
            copy_child(expression.children.front());
            return;
          }

          if (expression.operation == LogicalOperator::kExcept) {
            if (!is_empty(expression.children.back())) { return; }
            if (memo.Get(expression.children.front()).relations !=
                memo.Get(group).relations) {
              return;
            }
            memo.AddExpression(
                group, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                         .children = {expression.children.front()}});
            return;
          }

          // UNION DISTINCT: an empty branch can be removed, but the result
          // still needs duplicate elimination.
          std::vector<GroupId> survivors;
          for (GroupId child : expression.children) {
            if (!is_empty(child)) { survivors.push_back(child); }
          }
          if (survivors.empty()) { return; }
          std::vector<std::string> survivor_relations;
          for (GroupId child : survivors) {
            survivor_relations =
                UnionRelations(survivor_relations, memo.Get(child).relations);
          }
          if (survivor_relations != memo.Get(group).relations) { return; }
          if (survivors.size() == 1) {
            memo.AddExpression(
                group, LogicalExpression{.operation = LogicalOperator::kDistinct,
                                         .children = {survivors.front()}});
            return;
          }
          const GroupId union_all = memo.EnsureDerivedGroup(
              memo.Get(group).relations, "setop-empty-union-all");
          memo.AddExpression(
              union_all, LogicalExpression{.operation = LogicalOperator::kUnionAll,
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
              group,
              LogicalExpression{.operation = LogicalOperator::kDistinct,
                                .children = {union_all}});
        },
        LogicalOperator::kUnion));
    // Flatten nested UNION ALL branches into one n-ary append node.
    built.Add(Rule(
        "union_all_merge",
        Pattern::Op(LogicalOperator::kUnionAll, {}),
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
    return built;
  }();
  return rules;
}

std::string PhysicalProperties::Key() const {
  std::string key = require_row_position ? "rowpos:1" : "rowpos:0";
  key.append(wait_for_write_intent ? "|wait:1" : "|wait:0");
  for (const ColumnName& column : ordering) {
    key.push_back('|');
    key.append(column.ToString());
  }
  key.append("|lim:");
  key.append(std::to_string(limit_hint));
  key.append("|am:");
  key.append(std::to_string(static_cast<int>(access_method)));
  key.append("|dist:");
  key.append(std::to_string(static_cast<int>(distribution)));
  return key;
}

const RuleContext& RuleContext::Empty() {
  static const RuleContext empty;
  return empty;
}

std::vector<PlanAlternative> ImplementationRule::Apply(
    const Memo& memo, GroupId group, const LogicalExpression& expression,
    const std::vector<BestPlan>& children,
    const PhysicalProperties& properties, const RuleContext& context) const {
  if (!MayApply(expression.operation)) { return {};
}
  Bindings bindings;
  if (!pattern_.Match(memo, group, expression, &bindings)) { return {};
}
  return implement_(group, memo, bindings, expression, children,
                    properties, context);
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
    if (group == kInvalidGroup) { return;
}
    if (queued.insert(group).second) { queue.push_back(group);
}
  };
  enqueue(root);
  while (!queue.empty()) {
    const GroupId group = queue.front();
    queue.pop_front();
    ExploreGroup(group, enqueue);
    for (GroupId touched : memo_.DrainTouchedGroups()) { enqueue(touched);
}
  }
}

// Phase 7 worklist exploration: each group keeps a cursor over its
// expressions; rules appending expressions (here or in other groups) extend
// the work instead of rescanning settled state. No pass cap or convergence
// exception is needed because the memo is append-only and fingerprints
// deduplicate.
void SearchEngine::ExploreGroup(
    GroupId group, const std::function<void(GroupId)>& enqueue) {
  size_t& next = next_expression_[group];
  while (next < memo_.Get(group).expressions.size()) {
    const LogicalExpression expression = memo_.Get(group).expressions[next];
    ++next;
    for (const Rule& rule : rules_->Rules()) {
      if (!rule.MayApply(expression.operation)) { continue;
}
      // Transformation rules may legitimately decline; the return value is
      // only advisory for callers that track memo growth.
      std::ignore = rule.Apply(memo_, group, expression);
    }
    for (GroupId child : expression.children) { enqueue(child);
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
          if (!rule.MayApply(expression.operation)) { continue;
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
    case LogicalOperator::kRelational:
      return {};
    case LogicalOperator::kJoin:
    case LogicalOperator::kOuterJoin:
    case LogicalOperator::kCrossJoin:
    case LogicalOperator::kSemiJoin:
    case LogicalOperator::kAntiJoin:
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
      // TopN establishes ordering and consumes the complete child to choose
      // the best rows; neither an input ordering nor a limit hint is needed.
      PhysicalProperties child;
      child.require_row_position = required.require_row_position;
      child.wait_for_write_intent = required.wait_for_write_intent;
      child.access_method = required.access_method;
      return {child};
    }
    case LogicalOperator::kEmpty:
      // EmptyPlan needs the child schema but does not need to preserve any
      // physical property while producing zero rows.
      return {PhysicalProperties{}};
    case LogicalOperator::kValues:
    case LogicalOperator::kDummyScan:
      return {};
  }
  return {};
}

std::optional<BestPlan> SearchEngine::OptimizeGroup(  // NOLINT(misc-no-recursion) // Cascades branch-and-bound search recurses over memo groups by design; memo depth is bounded by the finite query.
    GroupId group, const PhysicalProperties& properties,
    const Implement& implement, const RuleContext& context) {
  const std::string cache_key = std::to_string(group) + '/' + properties.Key();
  if (const auto found = best_.find(cache_key); found != best_.end()) {
    return found->second;
  }

  const bool needs_ordering =
      !properties.ordering.empty() && context.query != nullptr &&
      !context.query->order_expressions_.empty();
  std::optional<BestPlan> best;
  const Group& memo_group = memo_.Get(group);
  for (size_t index = 0; index < memo_group.expressions.size(); ++index) {
    const LogicalExpression& expression = memo_group.expressions[index];
    const std::vector<PhysicalProperties> child_properties =
        RequiredChildProperties(expression, properties);
    if (child_properties.size() != expression.children.size()) { continue;
}
    std::vector<BestPlan> children;
    double child_cost = 0;
    double child_rows = 0;
    bool valid = true;
    for (size_t child = 0; child < expression.children.size(); ++child) {
      std::optional<BestPlan> child_plan = OptimizeGroup(
          expression.children[child], child_properties[child], implement,
          context);
      if (!child_plan) {
        valid = false;
        break;
      }
      child_cost += child_plan->cost;
      child_rows += child_plan->estimated_rows;
      children.push_back(std::move(*child_plan));
    }
    if (!valid) { continue;
}

    for (PlanAlternative alternative :
         implement(group, memo_, expression, children, properties,
              context)) {
      if (!alternative.plan) { continue;
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
        best = BestPlan{.plan=std::move(alternative.plan), .cost=cost,
                        .estimated_rows=alternative.estimated_rows, .group=group, .expression_index=index};
      }
    }
  }
  best_.emplace(cache_key, best);
  return best;
}

}  // namespace tinylamb::cascades
