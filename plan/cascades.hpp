/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_CASCADES_HPP
#define TINYLAMB_PLAN_CASCADES_HPP

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "plan/plan.hpp"
#include "type/column_name.hpp"

namespace tinylamb::cascades {

using GroupId = size_t;
constexpr GroupId kInvalidGroup = static_cast<GroupId>(-1);

enum class LogicalOperator { kScan, kJoin };

struct LogicalExpression {
  LogicalOperator operation{LogicalOperator::kScan};
  std::vector<GroupId> children;
  std::string table;

  [[nodiscard]] std::string Fingerprint() const;
};

struct Group {
  GroupId id{kInvalidGroup};
  std::vector<std::string> relations;
  std::vector<LogicalExpression> expressions;
};

class Memo {
 public:
  GroupId Build(const std::vector<std::string>& relations);
  GroupId EnsureGroup(std::vector<std::string> relations);
  bool AddExpression(GroupId group, LogicalExpression expression);

  [[nodiscard]] const Group& Get(GroupId group) const;
  [[nodiscard]] Group& Get(GroupId group);
  [[nodiscard]] size_t GroupCount() const { return groups_.size(); }
  [[nodiscard]] size_t ExpressionCount(GroupId group) const;

 private:
  [[nodiscard]] static std::string GroupKey(
      const std::vector<std::string>& relations);
  std::vector<Group> groups_;
  std::unordered_map<std::string, GroupId> groups_by_key_;
};

using Bindings = std::unordered_map<std::string, GroupId>;

// Relational rule patterns intentionally refer only to logical operators and
// child groups. They do not include Plan, Executor, catalog, or cost classes.
class Pattern {
 public:
  static Pattern Any(std::string capture = {});
  static Pattern Op(LogicalOperator operation, std::vector<Pattern> children,
                    std::string capture = {});

  [[nodiscard]] bool Match(const Memo& memo, GroupId group,
                           const LogicalExpression& expression,
                           Bindings* bindings) const;

 private:
  [[nodiscard]] bool MatchGroup(const Memo& memo, GroupId group,
                                Bindings* bindings) const;
  std::optional<LogicalOperator> operation_;
  std::vector<Pattern> children_;
  std::string capture_;
};

namespace dsl {
inline Pattern Any(std::string capture = {}) {
  return Pattern::Any(std::move(capture));
}
inline Pattern Scan(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kScan, {}, std::move(capture));
}
inline Pattern Join(Pattern left = Any(), Pattern right = Any(),
                    std::string capture = {}) {
  std::vector<Pattern> children;
  children.push_back(std::move(left));
  children.push_back(std::move(right));
  return Pattern::Op(LogicalOperator::kJoin, std::move(children),
                     std::move(capture));
}
}  // namespace dsl

class Rule {
 public:
  using Transform = std::function<void(const Bindings&, Memo&, GroupId,
                                       const LogicalExpression&)>;

  Rule(std::string name, Pattern pattern, Transform transform)
      : name_(std::move(name)),
        pattern_(std::move(pattern)),
        transform_(std::move(transform)) {}

  [[nodiscard]] const std::string& Name() const { return name_; }
  [[nodiscard]] bool Apply(Memo& memo, GroupId group,
                           const LogicalExpression& expression) const;

 private:
  std::string name_;
  Pattern pattern_;
  Transform transform_;
};

class RuleSet {
 public:
  RuleSet& Add(Rule rule);
  bool Remove(std::string_view name);
  [[nodiscard]] bool Contains(std::string_view name) const;
  [[nodiscard]] const std::vector<Rule>& Rules() const { return rules_; }

  static RuleSet Default();

 private:
  std::vector<Rule> rules_;
};

struct PhysicalProperties {
  bool require_row_position{false};
  std::vector<ColumnName> ordering;

  [[nodiscard]] std::string Key() const;
  bool operator==(const PhysicalProperties&) const = default;
};

struct PlanAlternative {
  Plan plan;
  double local_cost{0};
};

class ImplementationRule {
 public:
  using Implement = std::function<std::vector<PlanAlternative>(
      const Bindings&, const LogicalExpression&,
      const std::vector<struct BestPlan>&, const PhysicalProperties&)>;

  ImplementationRule(std::string name, Pattern pattern, Implement implement)
      : name_(std::move(name)),
        pattern_(std::move(pattern)),
        implement_(std::move(implement)) {}

  [[nodiscard]] const std::string& Name() const { return name_; }
  [[nodiscard]] std::vector<PlanAlternative> Apply(
      const Memo& memo, GroupId group, const LogicalExpression& expression,
      const std::vector<struct BestPlan>& children,
      const PhysicalProperties& properties) const;

 private:
  std::string name_;
  Pattern pattern_;
  Implement implement_;
};

class ImplementationRuleSet {
 public:
  ImplementationRuleSet& Add(ImplementationRule rule);
  bool Remove(std::string_view name);
  [[nodiscard]] bool Contains(std::string_view name) const;
  [[nodiscard]] const std::vector<ImplementationRule>& Rules() const {
    return rules_;
  }

 private:
  std::vector<ImplementationRule> rules_;
};

struct BestPlan {
  Plan plan;
  double cost{0};
  GroupId group{kInvalidGroup};
  size_t expression_index{0};
};

class SearchEngine {
 public:
  using Implement = std::function<std::vector<PlanAlternative>(
      GroupId, const LogicalExpression&, const std::vector<BestPlan>&,
      const PhysicalProperties&)>;

  SearchEngine(Memo memo, RuleSet rules)
      : memo_(std::move(memo)), rules_(std::move(rules)) {}

  void Explore(GroupId root);
  [[nodiscard]] std::optional<BestPlan> Optimize(
      GroupId root, const PhysicalProperties& properties,
      const Implement& implement);
  [[nodiscard]] std::optional<BestPlan> Optimize(
      GroupId root, const PhysicalProperties& properties,
      const ImplementationRuleSet& implementation_rules);

  [[nodiscard]] const Memo& GetMemo() const { return memo_; }
  [[nodiscard]] Memo& GetMemo() { return memo_; }

 private:
  void ExploreGroup(GroupId group);
  [[nodiscard]] std::optional<BestPlan> OptimizeGroup(
      GroupId group, const PhysicalProperties& properties,
      const Implement& implement);

  Memo memo_;
  RuleSet rules_;
  std::unordered_set<GroupId> explored_;
  std::unordered_set<GroupId> exploring_;
  std::unordered_map<std::string, std::optional<BestPlan>> best_;
};

}  // namespace tinylamb::cascades

#endif  // TINYLAMB_PLAN_CASCADES_HPP
