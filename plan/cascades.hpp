/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_CASCADES_HPP
#define TINYLAMB_PLAN_CASCADES_HPP

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
#include <vector>

#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class TransactionContext;
class Table;
class TableStatistics;
struct QueryData;
}  // namespace tinylamb

namespace tinylamb::cascades {

using GroupId = size_t;
constexpr GroupId kInvalidGroup = static_cast<GroupId>(-1);

enum class LogicalOperator : uint8_t {
  kScan,
  kJoin,
  kOuterJoin,
  kCrossJoin,
  kSemiJoin,
  kAntiJoin,
  kSingleJoin,
  kMarkJoin,
  kSelection,
  kProjection,
  kAggregation,
  kSort,
  kTopN,
  kDistinct,
  kMax1Row,
  kUnion,
  kUnionAll,
  kIntersect,
  kIntersectAll,
  kExcept,
  kExceptAll,
  kLimit,
  kEmpty,
  kValues,
  kConstantTable,
  kDummyScan,
  kWindow,
  kUnnest,
  kGenerateSeries,
  kRecursiveCte,
  kWorkTableScan,
  kMaterialize,
  kEagerSpool,
  kLazySpool,
  kExpand,
  kApply,
  kExchange,
  kGather,
  kBroadcast,
  kRedistribute,
  kSample,
  kAssert,
  // Opaque relational IR (outer/semi/anti joins, subqueries and CTEs). The
  // memo can cost/select it while its internal lowering remains specialized.
  kRelational,
};

// One logical expression inside a memo group. `predicate` carries the
// Selection predicate or the Join condition; `target_list` carries the
// Projection / Aggregation outputs; the limit fields configure kLimit.
// Scans keep their filter on the owning Group (see Group::filter) so every
// alternative of the group stays semantically consistent.
struct LogicalExpression {
  LogicalOperator operation{LogicalOperator::kScan};
  std::vector<GroupId> children{};
  std::string table{};
  std::optional<Expression> predicate{std::nullopt};
  std::vector<NamedExpression> target_list{};
  std::vector<bool> sort_ascending{};
  std::vector<std::optional<bool>> sort_nulls_first{};
  std::vector<Row> values{};
  size_t limit_count{0};
  size_t limit_offset{0};
  // Opaque JoinKind value for kOuterJoin (0 = LEFT, 1 = RIGHT, 2 = FULL).
  uint8_t join_type{0};
  std::shared_ptr<const SelectStatement> relational_statement{};
  Schema output_schema{};

  // Extended payload fields for Window / Grouping / Marker / Assertion:
  std::string marker_column{};
  std::vector<Expression> partition_by{};
  std::vector<Expression> grouping_sets{};
  std::string unnest_alias{};
  std::string offset_alias{};
  std::string cte_name{};
  double sample_rate{1.0};
  bool is_bernoulli{false};
  size_t depth_limit{0};
  std::optional<RecursiveDepthSpec> depth_spec{};

  [[nodiscard]] std::string Fingerprint() const;
};

struct Group {
  GroupId id{kInvalidGroup};
  std::vector<std::string> relations;
  std::vector<LogicalExpression> expressions;
  // Single-relation conjuncts of the query predicate, applied by every scan
  // implementation of this group (D1: the group, not the expression, owns the
  // scan filter so all alternatives filter identically).
  Expression filter;
  // Bitset over the memo-wide relation index (D5 join-graph metadata).
  uint64_t relation_mask{0};
  // Non-empty for derived root-layer groups (Selection/Projection/
  // Aggregation/Limit chains); distinguishes groups that share a relation set.
  std::string tag;
};

// A WHERE conjunct together with the relations it touches (D5).
struct ConjunctInfo {
  Expression conjunct;
  std::vector<std::string> relations;
};

class Memo {
 public:
  // Default per-group expression bound (Phase 7); overriding it is meant for
  // tests that need to hit the cap quickly.
  static constexpr size_t kDefaultExpressionCap = 4096;

  explicit Memo(size_t expression_cap = kDefaultExpressionCap)
      : expression_cap_(expression_cap) {}

  GroupId Build(const std::vector<std::string>& relations);
  GroupId Build(const std::vector<std::string>& relations,
                const std::vector<ConjunctInfo>& conjuncts);
  GroupId EnsureGroup(std::vector<std::string> relations);
  // Root-layer groups for Selection/Projection/Aggregation/Limit chains. They
  // share the relation set with their child group but carry a distinct tag so
  // group keys stay unique (D1).
  GroupId EnsureDerivedGroup(const std::vector<std::string>& relations,
                             std::string_view tag);
  bool AddExpression(GroupId group, LogicalExpression expression);

  [[nodiscard]] const Group& Get(GroupId group) const;
  [[nodiscard]] Group& Get(GroupId group);
  [[nodiscard]] size_t GroupCount() const { return groups_.size(); }
  [[nodiscard]] size_t ExpressionCount(GroupId group) const;

  // Constructs a Join expression whose condition payload is the canonical set
  // of stored conjuncts for this (left, right) split: every conjunct covered
  // by the union but by neither side. All default join rules must create
  // joins through this helper; it guarantees each conjunct is applied exactly
  // once on every root-to-leaf path.
  [[nodiscard]] LogicalExpression NewJoin(GroupId left, GroupId right) const;
  // Merges `predicate` into a single-relation group's scan filter (used by
  // pushdown rules); idempotent through conjunct canonicalization.
  void MergeScanFilter(GroupId group, const Expression& predicate);

  // D5 connectivity: true when some stored conjunct crosses the cut between
  // `left_mask` and its complement within `within_mask`.
  [[nodiscard]] bool CutConnected(uint64_t left_mask,
                                  uint64_t within_mask) const;
  [[nodiscard]] bool HasConjuncts() const { return !conjuncts_.empty(); }
  // True when no conjunct connects any pair of relations.
  [[nodiscard]] bool JoinGraphDisconnected() const;

  // Bitset of `relations` over the memo-wide relation index.
  [[nodiscard]] uint64_t RelationMask(
      const std::vector<std::string>& relations) const;

  // Groups that received new expressions since the last drain; the search
  // engine re-enqueues them (Phase 7 worklist).
  std::vector<GroupId> DrainTouchedGroups();
  // Expression cap exceeded: exploration degrades gracefully by keeping the
  // expressions discovered so far (documented degradation path).
  [[nodiscard]] bool Degraded() const { return degraded_; }

  // Provide table schemas so Scan expressions carry output_schema for
  // constraint-based optimization rules.
  void SetTableSchemas(const std::unordered_map<std::string, Schema>& schemas) {
    table_schemas_ = schemas;
  }

  void Dump(std::ostream& out) const;

  // Conjuncts whose touched relations are exactly this group's relation.
  [[nodiscard]] Expression ScanFilterFor(const Group& group) const;
  [[nodiscard]] Expression JoinConditionFor(const Group& left,
                                            const Group& right) const;

 private:
  [[nodiscard]] static std::string GroupKey(
      const std::vector<std::string>& relations);

  std::vector<Group> groups_;
  std::unordered_map<std::string, GroupId> groups_by_key_;
  std::unordered_map<std::string, size_t> relation_index_;
  std::vector<Expression> conjuncts_;
  std::vector<uint64_t> conjunct_masks_;
  std::vector<GroupId> touched_groups_;
  std::unordered_map<std::string, Schema> table_schemas_;
  const size_t expression_cap_;
  bool degraded_{false};
};

using Bindings = std::unordered_map<std::string, GroupId>;

// Relational rule patterns intentionally refer only to logical operators and
// child groups. They do not include Plan, Executor, catalog, or cost classes.
struct PayloadConstraint {
  // The matched expression must carry a predicate (Selection / Join with a
  // condition).
  bool requires_predicate{false};
  // The predicate's touched relations must be a subset of the relations of
  // this child group. Qualified column names are resolved against the child's
  // relation set; unqualified names cannot be proven to belong to the child
  // and therefore fail the constraint (strict interpretation).
  std::optional<size_t> predicate_within_child;
};

class Pattern {
 public:
  static Pattern Any(std::string capture = {});
  static Pattern Op(LogicalOperator operation, std::vector<Pattern> children,
                    std::string capture = {});
  static Pattern Op(LogicalOperator operation, std::vector<Pattern> children,
                    std::string capture, PayloadConstraint payload);

  [[nodiscard]] bool Match(const Memo& memo, GroupId group,
                           const LogicalExpression& expression,
                           Bindings* bindings) const;

 private:
  [[nodiscard]] bool MatchGroup(const Memo& memo, GroupId group,
                                Bindings* bindings) const;
  [[nodiscard]] bool MatchPayload(const Memo& memo,
                                  const LogicalExpression& expression,
                                  const Bindings& bindings) const;
  std::optional<LogicalOperator> operation_;
  std::vector<Pattern> children_;
  std::string capture_;
  PayloadConstraint payload_;
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
inline Pattern OuterJoin(Pattern left = Any(), Pattern right = Any(),
                         std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kOuterJoin,
                     {std::move(left), std::move(right)}, std::move(capture));
}
inline Pattern CrossJoin(Pattern left = Any(), Pattern right = Any(),
                         std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kCrossJoin,
                     {std::move(left), std::move(right)}, std::move(capture));
}
inline Pattern SemiJoin(Pattern left = Any(), Pattern right = Any(),
                        std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kSemiJoin,
                     {std::move(left), std::move(right)}, std::move(capture));
}
inline Pattern AntiJoin(Pattern left = Any(), Pattern right = Any(),
                        std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kAntiJoin,
                     {std::move(left), std::move(right)}, std::move(capture));
}
// Matches any Selection regardless of predicate content.
inline Pattern Selection(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kSelection, {std::move(child)},
                     std::move(capture));
}
// Matches a Selection whose predicate only touches the relations of child
// group `predicate_within_child`.
inline Pattern SelectionWithin(size_t predicate_within_child,
                               Pattern child = Any(),
                               std::string capture = {}) {
  PayloadConstraint payload;
  payload.requires_predicate = true;
  payload.predicate_within_child = predicate_within_child;
  return Pattern::Op(LogicalOperator::kSelection, {std::move(child)},
                     std::move(capture), payload);
}
inline Pattern Projection(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kProjection, {std::move(child)},
                     std::move(capture));
}
inline Pattern Aggregation(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kAggregation, {std::move(child)},
                     std::move(capture));
}
inline Pattern Sort(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kSort, {std::move(child)},
                     std::move(capture));
}
inline Pattern TopN(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kTopN, {std::move(child)},
                     std::move(capture));
}
inline Pattern Values(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kValues, {}, std::move(capture));
}
inline Pattern DummyScan(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kDummyScan, {}, std::move(capture));
}
inline Pattern Distinct(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kDistinct, {std::move(child)},
                     std::move(capture));
}
inline Pattern Max1Row(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kMax1Row, {std::move(child)},
                     std::move(capture));
}
// Set operations intentionally accept any arity; the SQL surface commonly
// represents a chain as a single n-ary logical expression.
inline Pattern Union(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kUnion, {}, std::move(capture));
}
inline Pattern UnionAll(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kUnionAll, {}, std::move(capture));
}
inline Pattern Intersect(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kIntersect, {}, std::move(capture));
}
inline Pattern IntersectAll(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kIntersectAll, {}, std::move(capture));
}
inline Pattern Except(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kExcept, {}, std::move(capture));
}
inline Pattern ExceptAll(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kExceptAll, {}, std::move(capture));
}
inline Pattern Empty(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kEmpty, {std::move(child)},
                     std::move(capture));
}
inline Pattern Limit(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kLimit, {std::move(child)},
                     std::move(capture));
}
inline Pattern SingleJoin(Pattern left = Any(), Pattern right = Any(),
                          std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kSingleJoin,
                     {std::move(left), std::move(right)}, std::move(capture));
}
inline Pattern MarkJoin(Pattern left = Any(), Pattern right = Any(),
                        std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kMarkJoin,
                     {std::move(left), std::move(right)}, std::move(capture));
}
inline Pattern ConstantTable(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kConstantTable, {}, std::move(capture));
}
inline Pattern Window(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kWindow, {std::move(child)},
                     std::move(capture));
}
inline Pattern Unnest(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kUnnest, {std::move(child)},
                     std::move(capture));
}
inline Pattern GenerateSeries(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kGenerateSeries, {}, std::move(capture));
}
inline Pattern RecursiveCte(Pattern anchor = Any(), Pattern recursive = Any(),
                            std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kRecursiveCte,
                     {std::move(anchor), std::move(recursive)},
                     std::move(capture));
}
inline Pattern WorkTableScan(std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kWorkTableScan, {}, std::move(capture));
}
inline Pattern Materialize(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kMaterialize, {std::move(child)},
                     std::move(capture));
}
inline Pattern EagerSpool(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kEagerSpool, {std::move(child)},
                     std::move(capture));
}
inline Pattern LazySpool(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kLazySpool, {std::move(child)},
                     std::move(capture));
}
inline Pattern Expand(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kExpand, {std::move(child)},
                     std::move(capture));
}
inline Pattern UnaryApply(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kApply, {std::move(child)},
                     std::move(capture));
}
inline Pattern Apply(Pattern left = Any(), Pattern right = Any(),
                     std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kApply,
                     {std::move(left), std::move(right)}, std::move(capture));
}
inline Pattern Exchange(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kExchange, {std::move(child)},
                     std::move(capture));
}
inline Pattern Gather(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kGather, {std::move(child)},
                     std::move(capture));
}
inline Pattern Broadcast(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kBroadcast, {std::move(child)},
                     std::move(capture));
}
inline Pattern Redistribute(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kRedistribute, {std::move(child)},
                     std::move(capture));
}
inline Pattern Sample(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kSample, {std::move(child)},
                     std::move(capture));
}
inline Pattern Assert(Pattern child = Any(), std::string capture = {}) {
  return Pattern::Op(LogicalOperator::kAssert, {std::move(child)},
                     std::move(capture));
}
}  // namespace dsl

class Rule {
 public:
  using Transform = std::function<void(const Bindings&, Memo&, GroupId,
                                       const LogicalExpression&)>;

  // `target` is an applicability hint (Phase 7): the rule never matches
  // expressions of other operator kinds, so join rules skip Scan/Selection
  // nodes without running their patterns.
  Rule(std::string name, Pattern pattern, Transform transform,
       std::optional<LogicalOperator> target = std::nullopt)
      : name_(std::move(name)),
        pattern_(std::move(pattern)),
        transform_(std::move(transform)),
        target_(target) {}

  [[nodiscard]] const std::string& Name() const { return name_; }
  [[nodiscard]] bool MayApply(LogicalOperator operation) const {
    return !target_ || *target_ == operation;
  }
  [[nodiscard]] bool Apply(Memo& memo, GroupId group,
                           const LogicalExpression& expression) const;

 private:
  std::string name_;
  Pattern pattern_;
  Transform transform_;
  std::optional<LogicalOperator> target_;
};

class RuleSet {
 public:
  RuleSet& Add(Rule rule);
  bool Remove(std::string_view name);
  [[nodiscard]] bool Contains(std::string_view name) const;
  [[nodiscard]] const std::vector<Rule>& Rules() const { return rules_; }
  [[nodiscard]] std::vector<std::string> Names() const;

  static const RuleSet& Default();

 private:
  std::vector<Rule> rules_;
};

// Explicit, immutable-per-search context handed to implementation rules
// (D4). Replaces the former thread-local closure: no mutable shared state,
// reentrant on one thread, safe under concurrent queries.
struct RuleContext {
  TransactionContext* transaction{nullptr};
  const QueryData* query{nullptr};
  std::unordered_map<std::string, std::shared_ptr<Table>> tables;
  std::unordered_map<std::string, std::shared_ptr<TableStatistics>> statistics;
  // Required columns per relation, flowing into scan implementation rules
  // (Phase 3): the optimizer computes them once from the whole query.
  std::unordered_map<std::string, std::vector<NamedExpression>>
      scan_projections;

  static const RuleContext& Empty();
};

enum class AccessMethod : uint8_t { kAny, kPreferIndex };
// Reserved for future distributed execution (Phase 5); never differs today
// but participates in property cache keys already.
enum class Distribution : uint8_t { kAny, kSingleNode };

enum class JoinMultiplicity : uint8_t {
  kUnknown,
  kOneToOne,
  kOneToMany,
  kManyToMany
};

struct PhysicalProperties {
  bool require_row_position{false};
  bool wait_for_write_intent{true};
  std::vector<ColumnName> ordering;
  std::vector<bool> sort_ascending{};
  std::vector<std::optional<bool>> sort_nulls_first{};
  std::string collation{};
  // Upper bound on interesting output rows (OFFSET + LIMIT) when the required
  // ordering is delivered; enables Top-K costing on ordered scans.
  size_t limit_hint{std::numeric_limits<size_t>::max()};
  AccessMethod access_method{AccessMethod::kAny};
  Distribution distribution{Distribution::kAny};
  bool distinct{false};
  std::vector<ColumnName> partition_by{};
  std::vector<ColumnName> bloom_filter_keys{};
  bool is_unique{false};
  JoinMultiplicity join_multiplicity{JoinMultiplicity::kUnknown};

  [[nodiscard]] std::string Key() const;
  bool operator==(const PhysicalProperties&) const = default;
};

struct JoinCardinalityEstimate {
  double rows{0.0};
  JoinMultiplicity multiplicity{JoinMultiplicity::kUnknown};
};

[[nodiscard]] JoinCardinalityEstimate EstimateJoinCardinality(
    double left_rows, double right_rows, bool left_is_unique,
    bool right_is_unique, double selectivity = 1.0);

enum class OperatorCostKind : uint8_t {
  kHashJoin,
  kMergeJoin,
  kNestedLoopJoin,
  kIndexScan,
  kBitmapScan,
  kSort
};

[[nodiscard]] double EstimateMultiColumnSelectivity(
    const std::vector<double>& selectivities, double correlation_factor = 0.0);

enum class PatternMatchingKind : uint8_t { kLike, kRegexp };

[[nodiscard]] double EstimatePatternSelectivity(
    PatternMatchingKind kind, std::string_view pattern,
    double domain_cardinality = 1000.0);

struct HistogramBucket {
  double lower{0.0};
  double upper{0.0};
  double count{0.0};
  double distinct_count{0.0};
};

[[nodiscard]] double EstimateHistogramJoinCardinality(
    const std::vector<HistogramBucket>& left_buckets,
    const std::vector<HistogramBucket>& right_buckets);

[[nodiscard]] double EstimateStarJoinCost(
    double fact_rows, const std::vector<double>& dimension_rows,
    const std::vector<double>& selectivities = {});

struct MemoryBudget {
  double max_memory_bytes{64.0 * 1024.0 * 1024.0};
  double row_size_bytes{64.0};
  double io_spill_cost_multiplier{3.5};
};

[[nodiscard]] double EstimateMemorySpillCost(OperatorCostKind kind,
                                             double input_rows,
                                             const MemoryBudget& budget = {});

[[nodiscard]] double CalibrateOperatorCost(OperatorCostKind kind,
                                           double input_rows_left,
                                           double input_rows_right,
                                           const PhysicalProperties& delivered,
                                           const PhysicalProperties& required);

struct PlanAlternative {
  Plan plan;
  double local_cost{0};
  // Estimated output rows of this alternative (Phase 6); parents combine it
  // into join cardinalities and sort penalties.
  double estimated_rows{0};
};

class ImplementationRule {
 public:
  using Implement = std::function<std::vector<PlanAlternative>(
      GroupId, const Memo&, const Bindings&, const LogicalExpression&,
      const std::vector<struct BestPlan>&, const PhysicalProperties&,
      const RuleContext&)>;

  ImplementationRule(std::string name, Pattern pattern, Implement implement,
                     std::optional<LogicalOperator> target = std::nullopt)
      : name_(std::move(name)),
        pattern_(std::move(pattern)),
        implement_(std::move(implement)),
        target_(target) {}

  [[nodiscard]] const std::string& Name() const { return name_; }
  [[nodiscard]] bool MayApply(LogicalOperator operation) const {
    return !target_ || *target_ == operation;
  }
  [[nodiscard]] std::vector<PlanAlternative> Apply(
      const Memo& memo, GroupId group, const LogicalExpression& expression,
      const std::vector<struct BestPlan>& children,
      const PhysicalProperties& properties, const RuleContext& context) const;

 private:
  std::string name_;
  Pattern pattern_;
  Implement implement_;
  std::optional<LogicalOperator> target_;
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
  double estimated_rows{0};
  GroupId group{kInvalidGroup};
  size_t expression_index{0};
};

class SearchEngine {
 public:
  using Implement = std::function<std::vector<PlanAlternative>(
      GroupId, const Memo&, const LogicalExpression&,
      const std::vector<BestPlan>&, const PhysicalProperties&,
      const RuleContext&)>;

  SearchEngine(Memo memo, const RuleSet& rules)
      : memo_(std::move(memo)), rules_(&rules) {}

  void Explore(GroupId root);
  [[nodiscard]] std::optional<BestPlan> Optimize(
      GroupId root, const PhysicalProperties& properties,
      const Implement& implement, const RuleContext& context);
  [[nodiscard]] std::optional<BestPlan> Optimize(
      GroupId root, const PhysicalProperties& properties,
      const ImplementationRuleSet& implementation_rules,
      const RuleContext& context);
  // Legacy overload without a context (empty RuleContext).
  [[nodiscard]] std::optional<BestPlan> Optimize(
      GroupId root, const PhysicalProperties& properties,
      const ImplementationRuleSet& implementation_rules);

  [[nodiscard]] const Memo& GetMemo() const { return memo_; }
  [[nodiscard]] Memo& GetMemo() { return memo_; }

  // D2: per-operator derivation of child requirements. Only operators that
  // preserve row position or ordering forward those requirements; joins and
  // aggregations drop them.
  [[nodiscard]] static std::vector<PhysicalProperties> RequiredChildProperties(
      const LogicalExpression& expression, const PhysicalProperties& required);

 private:
  void ExploreGroup(GroupId group, const std::function<void(GroupId)>& enqueue);
  [[nodiscard]] std::optional<BestPlan> OptimizeGroup(
      GroupId group, const PhysicalProperties& properties,
      const Implement& implement, const RuleContext& context);

  Memo memo_;
  const RuleSet* rules_;
  std::unordered_map<GroupId, size_t> next_expression_;
  std::unordered_map<std::string, std::optional<BestPlan>> best_;
};

}  // namespace tinylamb::cascades

#endif  // TINYLAMB_PLAN_CASCADES_HPP
