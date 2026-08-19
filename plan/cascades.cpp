/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/cascades.hpp"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace tinylamb::cascades {
namespace {

std::vector<std::string> Normalize(std::vector<std::string> relations) {
  std::ranges::sort(relations);
  relations.erase(std::unique(relations.begin(), relations.end()),
                  relations.end());
  return relations;
}

std::vector<std::string> UnionRelations(const std::vector<std::string>& left,
                                        const std::vector<std::string>& right) {
  std::vector<std::string> result;
  std::ranges::set_union(left, right, std::back_inserter(result));
  return result;
}

}  // namespace

std::string LogicalExpression::Fingerprint() const {
  std::ostringstream out;
  out << static_cast<int>(operation) << ':' << table;
  for (GroupId child : children) out << ':' << child;
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
  if (relations.empty()) throw std::invalid_argument("empty join graph");
  if (Normalize(relations).size() != relations.size()) {
    throw std::invalid_argument("duplicate relation in join graph");
  }
  return EnsureGroup(relations);
}

GroupId Memo::EnsureGroup(std::vector<std::string> relations) {
  relations = Normalize(std::move(relations));
  if (relations.empty()) throw std::invalid_argument("empty memo group");
  const std::string key = GroupKey(relations);
  if (const auto found = groups_by_key_.find(key);
      found != groups_by_key_.end()) {
    return found->second;
  }

  const GroupId id = groups_.size();
  groups_by_key_.emplace(key, id);
  groups_.push_back(Group{id, relations, {}});
  if (relations.size() == 1) {
    AddExpression(
        id, LogicalExpression{LogicalOperator::kScan, {}, relations.front()});
    return id;
  }

  std::vector<std::string> left{relations.front()};
  std::vector<std::string> right(relations.begin() + 1, relations.end());
  const GroupId left_group = EnsureGroup(std::move(left));
  const GroupId right_group = EnsureGroup(std::move(right));
  AddExpression(id, LogicalExpression{
                        LogicalOperator::kJoin, {left_group, right_group}, {}});
  return id;
}

bool Memo::AddExpression(GroupId group, LogicalExpression expression) {
  Group& target = Get(group);
  if (expression.operation == LogicalOperator::kScan) {
    if (target.relations.size() != 1 ||
        target.relations.front() != expression.table ||
        !expression.children.empty()) {
      throw std::invalid_argument("scan does not belong to memo group");
    }
  } else {
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
  }
  const std::string fingerprint = expression.Fingerprint();
  if (std::ranges::any_of(target.expressions, [&](const auto& existing) {
        return existing.Fingerprint() == fingerprint;
      })) {
    return false;
  }
  target.expressions.push_back(std::move(expression));
  return true;
}

const Group& Memo::Get(GroupId group) const {
  if (group >= groups_.size()) throw std::out_of_range("memo group");
  return groups_[group];
}

Group& Memo::Get(GroupId group) {
  if (group >= groups_.size()) throw std::out_of_range("memo group");
  return groups_[group];
}

size_t Memo::ExpressionCount(GroupId group) const {
  return Get(group).expressions.size();
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

bool Pattern::MatchGroup(const Memo& memo, GroupId group,
                         Bindings* bindings) const {
  if (!capture_.empty()) {
    auto [iter, inserted] = bindings->emplace(capture_, group);
    if (!inserted && iter->second != group) return false;
  }
  if (!operation_) return true;
  for (const LogicalExpression& expression : memo.Get(group).expressions) {
    Bindings local = *bindings;
    if (Match(memo, group, expression, &local)) {
      *bindings = std::move(local);
      return true;
    }
  }
  return false;
}

bool Pattern::Match(const Memo& memo, GroupId group,
                    const LogicalExpression& expression,
                    Bindings* bindings) const {
  if (operation_ && expression.operation != *operation_) return false;
  if (!children_.empty() && children_.size() != expression.children.size()) {
    return false;
  }
  Bindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, group);
    if (!inserted && iter->second != group) return false;
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].MatchGroup(memo, expression.children[i], &local)) {
      return false;
    }
  }
  *bindings = std::move(local);
  return true;
}

bool Rule::Apply(Memo& memo, GroupId group,
                 const LogicalExpression& expression) const {
  Bindings bindings;
  if (!pattern_.Match(memo, group, expression, &bindings)) return false;
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

RuleSet RuleSet::Default() {
  using namespace dsl;
  RuleSet rules;
  rules.Add(Rule("join_commutativity", Join(Any("left"), Any("right")),
                 [](const Bindings&, Memo& memo, GroupId group,
                    const LogicalExpression& expression) {
                   memo.AddExpression(
                       group, LogicalExpression{LogicalOperator::kJoin,
                                                {expression.children[1],
                                                 expression.children[0]},
                                                {}});
                 }));
  rules.Add(Rule(
      "join_enumeration", Join(),
      [](const Bindings&, Memo& memo, GroupId group, const LogicalExpression&) {
        const std::vector<std::string> relations = memo.Get(group).relations;
        if (relations.size() < 3) return;
        if (relations.size() >= std::numeric_limits<uint64_t>::digits) {
          throw std::runtime_error("join graph is too large for enumeration");
        }
        const uint64_t limit = uint64_t{1} << relations.size();
        for (uint64_t mask = 1; mask + 1 < limit; ++mask) {
          // Commutativity supplies the mirrored orientation.
          if ((mask & 1U) == 0) continue;
          std::vector<std::string> left;
          std::vector<std::string> right;
          for (size_t i = 0; i < relations.size(); ++i) {
            ((mask >> i) & 1U ? left : right).push_back(relations[i]);
          }
          if (left.empty() || right.empty()) continue;
          const GroupId left_group = memo.EnsureGroup(std::move(left));
          const GroupId right_group = memo.EnsureGroup(std::move(right));
          memo.AddExpression(group, LogicalExpression{LogicalOperator::kJoin,
                                                      {left_group, right_group},
                                                      {}});
        }
      }));
  return rules;
}

std::string PhysicalProperties::Key() const {
  std::string key = require_row_position ? "rowpos:1" : "rowpos:0";
  for (const ColumnName& column : ordering) {
    key.push_back('|');
    key.append(column.ToString());
  }
  return key;
}

std::vector<PlanAlternative> ImplementationRule::Apply(
    const Memo& memo, GroupId group, const LogicalExpression& expression,
    const std::vector<BestPlan>& children,
    const PhysicalProperties& properties) const {
  Bindings bindings;
  if (!pattern_.Match(memo, group, expression, &bindings)) return {};
  return implement_(bindings, expression, children, properties);
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
  ExploreGroup(root);
  best_.clear();
}

void SearchEngine::ExploreGroup(GroupId group) {
  if (explored_.contains(group) || exploring_.contains(group)) return;
  exploring_.insert(group);
  for (size_t pass = 0; pass < 64; ++pass) {
    bool changed = false;
    const std::vector<LogicalExpression> snapshot =
        memo_.Get(group).expressions;
    for (const LogicalExpression& expression : snapshot) {
      for (const Rule& rule : rules_.Rules()) {
        changed |= rule.Apply(memo_, group, expression);
      }
    }
    if (!changed) break;
    if (pass == 63) throw std::runtime_error("cascades rules did not converge");
  }
  const std::vector<LogicalExpression> expressions =
      memo_.Get(group).expressions;
  for (const LogicalExpression& expression : expressions) {
    for (GroupId child : expression.children) ExploreGroup(child);
  }
  exploring_.erase(group);
  explored_.insert(group);
}

std::optional<BestPlan> SearchEngine::Optimize(
    GroupId root, const PhysicalProperties& properties,
    const Implement& implement) {
  Explore(root);
  return OptimizeGroup(root, properties, implement);
}

std::optional<BestPlan> SearchEngine::Optimize(
    GroupId root, const PhysicalProperties& properties,
    const ImplementationRuleSet& implementation_rules) {
  return Optimize(
      root, properties,
      [&](GroupId group, const LogicalExpression& expression,
          const std::vector<BestPlan>& children,
          const PhysicalProperties& required) {
        std::vector<PlanAlternative> alternatives;
        for (const ImplementationRule& rule : implementation_rules.Rules()) {
          std::vector<PlanAlternative> generated =
              rule.Apply(memo_, group, expression, children, required);
          alternatives.insert(alternatives.end(),
                              std::make_move_iterator(generated.begin()),
                              std::make_move_iterator(generated.end()));
        }
        return alternatives;
      });
}

std::optional<BestPlan> SearchEngine::OptimizeGroup(
    GroupId group, const PhysicalProperties& properties,
    const Implement& implement) {
  const std::string cache_key = std::to_string(group) + '/' + properties.Key();
  if (const auto found = best_.find(cache_key); found != best_.end()) {
    return found->second;
  }

  std::optional<BestPlan> best;
  const Group& memo_group = memo_.Get(group);
  for (size_t index = 0; index < memo_group.expressions.size(); ++index) {
    const LogicalExpression& expression = memo_group.expressions[index];
    std::vector<BestPlan> children;
    double child_cost = 0;
    bool valid = true;
    for (GroupId child : expression.children) {
      std::optional<BestPlan> child_plan =
          OptimizeGroup(child, properties, implement);
      if (!child_plan) {
        valid = false;
        break;
      }
      child_cost += child_plan->cost;
      children.push_back(std::move(*child_plan));
    }
    if (!valid) continue;

    for (PlanAlternative alternative :
         implement(group, expression, children, properties)) {
      if (!alternative.plan) continue;
      const double cost = child_cost + alternative.local_cost;
      if (!best || cost < best->cost) {
        best = BestPlan{std::move(alternative.plan), cost, group, index};
      }
    }
  }
  best_.emplace(cache_key, best);
  return best;
}

}  // namespace tinylamb::cascades
