/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/plan_memo_oracle.hpp"

#include <algorithm>
#include <sstream>
#include <unordered_set>
#include <utility>

#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "type/column.hpp"
#include "type/constraint.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

Expression FilterConjunct(std::mt19937& rng, const std::string& relation,
                          int index) {
  // tN.c{0,1} = <small const>; keeps the value domain tiny so fingerprints
  // stay readable in diagnostics.
  const std::string column =
      relation + ".c" + std::to_string(index % 2 == 0 ? 0 : 1);
  const int value = static_cast<int>(rng() % 3);
  return BinaryExpressionExp(
      ColumnValueExp(column), BinaryOperation::kEquals,
      ConstantValueExp(Value(static_cast<int64_t>(value))));
}

cascades::ConjunctInfo JoinConjunct(const std::string& left,
                                    const std::string& right) {
  Expression predicate = BinaryExpressionExp(ColumnValueExp(left + ".c0"),
                                             BinaryOperation::kEquals,
                                             ColumnValueExp(right + ".c0"));
  return {.conjunct = std::move(predicate), .relations = {left, right}};
}

std::string Describe(const GeneratedJoinGraph& graph) {
  std::ostringstream out;
  out << "relations{";
  for (size_t i = 0; i < graph.relations.size(); ++i) {
    if (i > 0) {
      out << ',';
    }
    out << graph.relations[i];
  }
  out << "} conjuncts{";
  for (size_t i = 0; i < graph.conjuncts.size(); ++i) {
    if (i > 0) {
      out << ';';
    }
    out << graph.conjuncts[i].conjunct->ToString() << " over[";
    for (size_t j = 0; j < graph.conjuncts[i].relations.size(); ++j) {
      if (j > 0) {
        out << ',';
      }
      out << graph.conjuncts[i].relations[j];
    }
    out << ']';
  }
  out << '}';
  return out.str();
}

// True when `child_masks` are pairwise disjoint and union to `parent_mask`.
bool MasksPartition(uint64_t parent_mask,
                    const std::vector<uint64_t>& child_masks) {
  uint64_t union_mask = 0;
  for (uint64_t mask : child_masks) {
    if (mask == 0 || (union_mask & mask) != 0) {
      return false;
    }
    union_mask |= mask;
  }
  return union_mask == parent_mask;
}

std::string PredicateText(const std::optional<Expression>& predicate) {
  if (!predicate.has_value() || !(*predicate)) {
    return "<null>";
  }
  return (*predicate)->ToString();
}

// NOLINTNEXTLINE(performance-unnecessary-value-param)
size_t ExploreRootCount(GeneratedJoinGraph graph,
                        const cascades::RuleSet& rules) {
  cascades::Memo memo;
  const cascades::GroupId root = memo.Build(graph.relations, graph.conjuncts);
  cascades::SearchEngine search(std::move(memo), rules);
  search.Explore(root);
  return search.GetMemo().ExpressionCount(root);
}

}  // namespace

GeneratedJoinGraph GenerateJoinGraph(std::mt19937& rng,
                                     const PlanMemoGenConfig& config) {
  const int span = std::max(1, config.max_relations - config.min_relations + 1);
  const int relation_count =
      config.min_relations +
      static_cast<int>(rng() % static_cast<uint32_t>(span));
  GeneratedJoinGraph graph;
  for (int i = 0; i < relation_count; ++i) {
    graph.relations.push_back("t" + std::to_string(i));
  }
  const int conjunct_count =
      static_cast<int>(rng() % static_cast<uint32_t>(config.max_conjuncts + 1));
  for (int i = 0; i < conjunct_count; ++i) {
    if (relation_count >= 2 && rng() % 2 == 0) {
      // Two-relation equality join between distinct relations.
      const size_t left_idx =
          static_cast<size_t>(rng()) % graph.relations.size();
      size_t right_idx =
          static_cast<size_t>(rng()) % (graph.relations.size() - 1);
      if (right_idx >= left_idx) {
        ++right_idx;
      }
      graph.conjuncts.push_back(
          JoinConjunct(graph.relations[left_idx], graph.relations[right_idx]));
    } else {
      // Single-relation filter on a random relation.
      const size_t idx = static_cast<size_t>(rng()) % graph.relations.size();
      Expression predicate = FilterConjunct(rng, graph.relations[idx], i);
      graph.conjuncts.push_back({std::move(predicate), {graph.relations[idx]}});
    }
  }
  return graph;
}

std::string CheckMemoInvariants(const cascades::Memo& memo,
                                bool check_conjunct_attachment) {
  for (size_t group_id = 0; group_id < memo.GroupCount(); ++group_id) {
    const cascades::Group& group = memo.Get(group_id);
    if (group.relations.empty()) {
      return "group " + std::to_string(group_id) + " has no relations";
    }
    // Relation sets must be unique (memo keys on the normalized set).
    std::unordered_set<std::string> seen(group.relations.begin(),
                                         group.relations.end());
    if (seen.size() != group.relations.size()) {
      return "group " + std::to_string(group_id) + " has duplicate relations";
    }
    uint64_t expected_mask = 0;
    try {
      expected_mask = memo.RelationMask(group.relations);
    } catch (const std::invalid_argument& e) {
      return "group " + std::to_string(group_id) +
             " references relations outside the join graph: " + e.what();
    }
    if (group.relation_mask != expected_mask) {
      return "group " + std::to_string(group_id) + " mask mismatch";
    }
    // Fingerprints must be unique within a group (D1: distinct payloads do
    // not collapse).
    std::unordered_set<std::string> fingerprints;
    for (const cascades::LogicalExpression& expression : group.expressions) {
      const std::string fingerprint = expression.Fingerprint();
      if (!fingerprints.insert(fingerprint).second) {
        return "group " + std::to_string(group_id) +
               " has duplicate expression fingerprint: " + fingerprint;
      }
      for (cascades::GroupId child : expression.children) {
        if (child >= memo.GroupCount()) {
          return "group " + std::to_string(group_id) +
                 " references unknown child group " + std::to_string(child);
        }
      }
      using Op = cascades::LogicalOperator;
      switch (expression.operation) {
        case Op::kScan: {
          if (!expression.children.empty()) {
            return "scan in group " + std::to_string(group_id) +
                   " must not have children";
          }
          if (group.relations.size() != 1) {
            return "scan in group " + std::to_string(group_id) +
                   " must sit in a single-relation group";
          }
          break;
        }
        case Op::kJoin:
        case Op::kCrossJoin:
        case Op::kOuterJoin: {
          if (expression.children.size() != 2) {
            return "join in group " + std::to_string(group_id) +
                   " must have two children";
          }
          const uint64_t left = memo.Get(expression.children[0]).relation_mask;
          const uint64_t right = memo.Get(expression.children[1]).relation_mask;
          if (!MasksPartition(group.relation_mask, {left, right})) {
            return "join in group " + std::to_string(group_id) +
                   " does not partition the parent relation set";
          }
          if (check_conjunct_attachment && expression.operation == Op::kJoin) {
            const std::string want = PredicateText(
                memo.JoinConditionFor(memo.Get(expression.children[0]),
                                      memo.Get(expression.children[1])));
            if (PredicateText(expression.predicate) != want) {
              return "join in group " + std::to_string(group_id) +
                     " condition diverges from stored conjuncts: got " +
                     PredicateText(expression.predicate) + " want " + want;
            }
          }
          break;
        }
        case Op::kSemiJoin:
        case Op::kAntiJoin: {
          if (expression.children.size() != 2) {
            return "semi/anti join in group " + std::to_string(group_id) +
                   " must have two children";
          }
          const uint64_t left = memo.Get(expression.children[0]).relation_mask;
          const uint64_t right = memo.Get(expression.children[1]).relation_mask;
          if (left != group.relation_mask &&
              !MasksPartition(group.relation_mask, {left, right})) {
            return "semi/anti join in group " + std::to_string(group_id) +
                   " does not match parent relation set";
          }
          break;
        }
        default: {
          // Single-child logical operators preserve the relation set.
          if (expression.children.size() == 1) {
            const uint64_t child =
                memo.Get(expression.children[0]).relation_mask;
            if (child != group.relation_mask) {
              return "unary operator in group " + std::to_string(group_id) +
                     " changes the relation set";
            }
          }
          break;
        }
      }
    }
    if (check_conjunct_attachment && group.relations.size() == 1 &&
        group.tag.empty()) {
      const std::string want = PredicateText(memo.ScanFilterFor(group));
      if (PredicateText(group.filter) != want) {
        return "scan filter in group " + std::to_string(group_id) +
               " diverges from stored conjuncts: got " +
               PredicateText(group.filter) + " want " + want;
      }
    }
  }
  return "";
}

std::string CheckExploreEquivalence(const GeneratedJoinGraph& graph) {
  const std::string where = Describe(graph);
  // 1. Fresh-build invariants (conjunct attachment must hold exactly).
  {
    cascades::Memo memo;
    cascades::GroupId root = 0;
    try {
      root = memo.Build(graph.relations, graph.conjuncts);
    } catch (const std::exception& e) {
      return "build threw for " + where + ": " + e.what();
    }
    if (const std::string problem = CheckMemoInvariants(memo, true);
        !problem.empty()) {
      return "fresh-build invariant failed for " + where + ": " + problem;
    }
    if (memo.Get(root).relations.size() != graph.relations.size()) {
      return "root covers wrong relation set for " + where;
    }
  }
  // 2. Post-explore structural invariants (filters may legitimately grow via
  // pushdown, so conjunct attachment is not re-checked here).
  size_t explored_root_count = 0;
  size_t explored_groups = 0;
  {
    cascades::Memo memo;
    const cascades::GroupId root = memo.Build(graph.relations, graph.conjuncts);
    cascades::SearchEngine search(std::move(memo),
                                  cascades::RuleSet::Default());
    search.Explore(root);
    if (const std::string problem =
            CheckMemoInvariants(search.GetMemo(), false);
        !problem.empty()) {
      return "post-explore invariant failed for " + where + ": " + problem;
    }
    if (search.GetMemo().ExpressionCount(root) == 0) {
      return "explore left root empty for " + where;
    }
    explored_root_count = search.GetMemo().ExpressionCount(root);
    explored_groups = search.GetMemo().GroupCount();
  }
  // 3. Determinism: rebuild + re-explore reproduces the shape.
  {
    cascades::Memo memo;
    const cascades::GroupId root = memo.Build(graph.relations, graph.conjuncts);
    cascades::SearchEngine search(std::move(memo),
                                  cascades::RuleSet::Default());
    search.Explore(root);
    if (search.GetMemo().ExpressionCount(root) != explored_root_count ||
        search.GetMemo().GroupCount() != explored_groups) {
      return "re-explore diverged for " + where;
    }
  }
  // 4. Monotonicity: fewer rules must not enumerate more alternatives.
  {
    cascades::RuleSet reduced = cascades::RuleSet::Default();
    reduced.Remove("join_associativity_left");
    reduced.Remove("join_associativity_right");
    const size_t reduced_count = ExploreRootCount(graph, reduced);
    if (reduced_count > explored_root_count) {
      return "rule-subset monotonicity failed for " + where +
             ": reduced=" + std::to_string(reduced_count) +
             " full=" + std::to_string(explored_root_count);
    }
  }
  return "";
}

GeneratedJoinGraph ShrinkJoinGraph(const GeneratedJoinGraph& graph) {
  if (CheckExploreEquivalence(graph).empty()) {
    return graph;
  }
  GeneratedJoinGraph current = graph;
  // Drop conjuncts one at a time while the mismatch is preserved.
  for (size_t i = 0; i < current.conjuncts.size();) {
    GeneratedJoinGraph candidate = current;
    candidate.conjuncts.erase(candidate.conjuncts.begin() +
                              static_cast<ptrdiff_t>(i));
    if (!CheckExploreEquivalence(candidate).empty()) {
      current = std::move(candidate);
    } else {
      ++i;
    }
  }
  // Drop relations (with conjuncts touching them) while the mismatch and at
  // least two relations are preserved.
  for (size_t i = 0; i < current.relations.size();) {
    if (current.relations.size() <= 2) {
      break;
    }
    GeneratedJoinGraph candidate = current;
    const std::string dropped = candidate.relations[i];
    candidate.relations.erase(candidate.relations.begin() +
                              static_cast<ptrdiff_t>(i));
    candidate.conjuncts.erase(
        std::remove_if(candidate.conjuncts.begin(), candidate.conjuncts.end(),
                       [&](const cascades::ConjunctInfo& info) {
                         return std::find(info.relations.begin(),
                                          info.relations.end(),
                                          dropped) != info.relations.end();
                       }),
        candidate.conjuncts.end());
    if (!CheckExploreEquivalence(candidate).empty()) {
      current = std::move(candidate);
    } else {
      ++i;
    }
  }
  return current;
}

GeneratedComplexMemo GenerateComplexMemo(std::mt19937& rng,
                                         const ComplexMemoGenConfig& config) {
  const int span = std::max(1, config.max_relations - config.min_relations + 1);
  const int relation_count =
      config.min_relations +
      static_cast<int>(rng() % static_cast<uint32_t>(span));

  GeneratedComplexMemo gen;
  for (int i = 0; i < relation_count; ++i) {
    const std::string rel = "t" + std::to_string(i);
    gen.relations.push_back(rel);
    std::vector<Column> cols;
    cols.emplace_back("c0", ValueType::kInt64,
                      Constraint(Constraint::kPrimaryKey));
    cols.emplace_back("c1", ValueType::kInt64, Constraint(Constraint::kUnique));
    cols.emplace_back("c2", ValueType::kInt64,
                      Constraint(Constraint::kNotNull));
    cols.emplace_back("c3", ValueType::kInt64,
                      Constraint(Constraint::kNothing));
    gen.schemas.emplace(rel, Schema(rel, std::move(cols)));
  }

  const int conjunct_count =
      static_cast<int>(rng() % static_cast<uint32_t>(config.max_conjuncts + 1));
  for (int i = 0; i < conjunct_count; ++i) {
    if (relation_count >= 2 && rng() % 2 == 0) {
      const size_t left_idx = static_cast<size_t>(rng()) % gen.relations.size();
      size_t right_idx =
          static_cast<size_t>(rng()) % (gen.relations.size() - 1);
      if (right_idx >= left_idx) {
        ++right_idx;
      }
      const std::string& left = gen.relations[left_idx];
      const std::string& right = gen.relations[right_idx];
      const std::string left_col = (rng() % 2 == 0) ? ".c0" : ".c1";
      const std::string right_col = (rng() % 2 == 0) ? ".c3" : ".c1";
      Expression pred = BinaryExpressionExp(ColumnValueExp(left + left_col),
                                            BinaryOperation::kEquals,
                                            ColumnValueExp(right + right_col));
      gen.conjuncts.push_back({std::move(pred), {left, right}});
    } else {
      const size_t idx = static_cast<size_t>(rng()) % gen.relations.size();
      const std::string& rel = gen.relations[idx];
      Expression pred;
      const int pred_kind = static_cast<int>(rng() % 4);
      if (pred_kind == 0) {
        pred = BinaryExpressionExp(
            ColumnValueExp(rel + ".c1"), BinaryOperation::kEquals,
            ConstantValueExp(Value(static_cast<int64_t>(rng() % 5))));
      } else if (pred_kind == 1) {
        pred = BinaryExpressionExp(ColumnValueExp(rel + ".c0"),
                                   BinaryOperation::kGreaterThan,
                                   ConstantValueExp(Value(int64_t{0})));
      } else if (pred_kind == 2) {
        pred = UnaryExpressionExp(ColumnValueExp(rel + ".c2"),
                                  UnaryOperation::kIsNotNull);
      } else {
        pred = UnaryExpressionExp(ColumnValueExp(rel + ".c3"),
                                  UnaryOperation::kIsNull);
      }
      gen.conjuncts.push_back({std::move(pred), {rel}});
    }
  }

  const int depth =
      1 + static_cast<int>(rng() % static_cast<uint32_t>(
                                       std::max(1, config.max_operator_depth)));
  std::vector<int> op_choices;
  op_choices.reserve(static_cast<size_t>(depth));
  for (int d = 0; d < depth; ++d) {
    op_choices.push_back(static_cast<int>(rng() % 7));
  }
  const bool use_outer_join_root = (relation_count >= 2 && rng() % 3 == 0);
  const auto outer_join_type = static_cast<uint8_t>(rng() % 3);

  std::ostringstream desc;
  desc << "relations{" << gen.relations.size() << "} ops[";
  for (int op : op_choices) {
    desc << op << ",";
  }
  desc << "] outer_root=" << use_outer_join_root;
  gen.description = desc.str();

  gen.builder = [relations = gen.relations, conjuncts = gen.conjuncts,
                 schemas = gen.schemas, op_choices, use_outer_join_root,
                 outer_join_type](cascades::Memo& memo) -> cascades::GroupId {
    memo.SetTableSchemas(schemas);
    cascades::GroupId current = memo.Build(relations, conjuncts);
    if (use_outer_join_root && relations.size() >= 2) {
      const std::vector<std::string> left_rels = {relations[0]};
      std::vector<std::string> right_rels(relations.begin() + 1,
                                          relations.end());
      const cascades::GroupId left_group = memo.EnsureGroup(left_rels);
      const cascades::GroupId right_group = memo.EnsureGroup(right_rels);
      current = memo.EnsureDerivedGroup(relations, "root_outer_join");
      Expression join_cond = BinaryExpressionExp(
          ColumnValueExp(left_rels[0] + ".c0"), BinaryOperation::kEquals,
          ColumnValueExp(right_rels[0] + ".c3"));
      memo.AddExpression(current,
                         cascades::LogicalExpression{
                             .operation = cascades::LogicalOperator::kOuterJoin,
                             .children = {left_group, right_group},
                             .predicate = std::move(join_cond),
                             .join_type = outer_join_type});
    }

    int step = 0;
    for (int op : op_choices) {
      const std::string tag = "op_" + std::to_string(step++) + "_";
      const std::string& rel0 = relations[0];
      switch (op) {
        case 0: {  // Selection
          const cascades::GroupId sel_group =
              memo.EnsureDerivedGroup(relations, tag + "selection");
          Expression p;
          if (relations.size() >= 2 && step % 2 == 0) {
            p = BinaryExpressionExp(
                BinaryExpressionExp(ColumnValueExp(relations[0] + ".c1"),
                                    BinaryOperation::kEquals,
                                    ConstantValueExp(Value(int64_t{1}))),
                BinaryOperation::kOr,
                BinaryExpressionExp(ColumnValueExp(relations[1] + ".c1"),
                                    BinaryOperation::kEquals,
                                    ConstantValueExp(Value(int64_t{2}))));
          } else {
            p = BinaryExpressionExp(ColumnValueExp(rel0 + ".c1"),
                                    BinaryOperation::kGreaterThan,
                                    ConstantValueExp(Value(int64_t{0})));
          }
          memo.AddExpression(
              sel_group, cascades::LogicalExpression{
                             .operation = cascades::LogicalOperator::kSelection,
                             .children = {current},
                             .predicate = std::move(p)});
          current = sel_group;
          break;
        }
        case 1: {  // Projection
          const cascades::GroupId proj_group =
              memo.EnsureDerivedGroup(relations, tag + "projection");
          memo.AddExpression(
              proj_group,
              cascades::LogicalExpression{
                  .operation = cascades::LogicalOperator::kProjection,
                  .children = {current},
                  .target_list = {NamedExpression(rel0 + ".c0"),
                                  NamedExpression(rel0 + ".c1")}});
          current = proj_group;
          break;
        }
        case 2: {  // Aggregation
          const cascades::GroupId agg_group =
              memo.EnsureDerivedGroup(relations, tag + "aggregation");
          memo.AddExpression(
              agg_group,
              cascades::LogicalExpression{
                  .operation = cascades::LogicalOperator::kAggregation,
                  .children = {current},
                  .target_list = {NamedExpression(rel0 + ".c0"),
                                  NamedExpression(
                                      "cnt",
                                      AggregateExpressionExp(
                                          AggregationType::kCount,
                                          ColumnValueExp(rel0 + ".c1")))},
                  .grouping_sets = {ColumnValueExp(rel0 + ".c0")}});
          current = agg_group;
          break;
        }
        case 3: {  // Distinct
          const cascades::GroupId dist_group =
              memo.EnsureDerivedGroup(relations, tag + "distinct");
          memo.AddExpression(
              dist_group, cascades::LogicalExpression{
                              .operation = cascades::LogicalOperator::kDistinct,
                              .children = {current}});
          current = dist_group;
          break;
        }
        case 4: {  // Sort
          const cascades::GroupId sort_group =
              memo.EnsureDerivedGroup(relations, tag + "sort");
          memo.AddExpression(sort_group,
                             cascades::LogicalExpression{
                                 .operation = cascades::LogicalOperator::kSort,
                                 .children = {current},
                                 .target_list = {NamedExpression(rel0 + ".c0")},
                                 .sort_ascending = {true},
                                 .sort_nulls_first = {std::nullopt}});
          current = sort_group;
          break;
        }
        case 5: {  // Limit
          const cascades::GroupId limit_group =
              memo.EnsureDerivedGroup(relations, tag + "limit");
          memo.AddExpression(limit_group,
                             cascades::LogicalExpression{
                                 .operation = cascades::LogicalOperator::kLimit,
                                 .children = {current},
                                 .limit_count = 5,
                                 .limit_offset = 0});
          current = limit_group;
          break;
        }
        default: {  // Window
          const cascades::GroupId win_group =
              memo.EnsureDerivedGroup(relations, tag + "window");
          memo.AddExpression(
              win_group, cascades::LogicalExpression{
                             .operation = cascades::LogicalOperator::kWindow,
                             .children = {current},
                             .partition_by = {ColumnValueExp(rel0 + ".c0")}});
          current = win_group;
          break;
        }
      }
    }
    return current;
  };

  return gen;
}

std::string CheckComplexMemoEquivalence(const GeneratedComplexMemo& gen) {
  constexpr size_t kFuzzCap = 128;
  // 1. Fresh-build invariants
  size_t explored_root_count = 0;
  size_t explored_groups = 0;
  {
    cascades::Memo memo(kFuzzCap);
    cascades::GroupId root = 0;
    try {
      root = gen.builder(memo);
    } catch (const std::exception& e) {
      return "builder threw for " + gen.description + ": " + e.what();
    }
    if (const std::string problem = CheckMemoInvariants(memo, false);
        !problem.empty()) {
      return "fresh-build invariant failed for " + gen.description + ": " +
             problem;
    }
    if (memo.Get(root).relations.size() != gen.relations.size()) {
      return "root covers wrong relation set for " + gen.description;
    }
  }

  // 2. Exploration with Default rule set (all 105 rules)
  {
    cascades::Memo memo(kFuzzCap);
    const cascades::GroupId root = gen.builder(memo);
    cascades::SearchEngine search(std::move(memo),
                                  cascades::RuleSet::Default());
    search.Explore(root);
    if (const std::string problem =
            CheckMemoInvariants(search.GetMemo(), false);
        !problem.empty()) {
      return "post-explore invariant failed for " + gen.description + ": " +
             problem;
    }
    if (search.GetMemo().ExpressionCount(root) == 0) {
      return "explore left root empty for " + gen.description;
    }
    explored_root_count = search.GetMemo().ExpressionCount(root);
    explored_groups = search.GetMemo().GroupCount();
  }

  // 3. Determinism: rebuild + re-explore gives identical counts
  {
    cascades::Memo memo(kFuzzCap);
    const cascades::GroupId root = gen.builder(memo);
    cascades::SearchEngine search(std::move(memo),
                                  cascades::RuleSet::Default());
    search.Explore(root);
    if (search.GetMemo().ExpressionCount(root) != explored_root_count ||
        search.GetMemo().GroupCount() != explored_groups) {
      return "determinism failed for " + gen.description + ": first(" +
             std::to_string(explored_root_count) + "," +
             std::to_string(explored_groups) + ") second(" +
             std::to_string(search.GetMemo().ExpressionCount(root)) + "," +
             std::to_string(search.GetMemo().GroupCount()) + ")";
    }
  }

  // 4. Rule-subset monotonicity
  {
    cascades::RuleSet reduced = cascades::RuleSet::Default();
    reduced.Remove("join_associativity_left");
    reduced.Remove("join_associativity_right");
    cascades::Memo memo(kFuzzCap);
    const cascades::GroupId root = gen.builder(memo);
    cascades::SearchEngine search(std::move(memo), reduced);
    search.Explore(root);
    if (const std::string problem =
            CheckMemoInvariants(search.GetMemo(), false);
        !problem.empty()) {
      return "subset-explore invariant failed for " + gen.description + ": " +
             problem;
    }
    if (search.GetMemo().ExpressionCount(root) > explored_root_count) {
      return "rule-subset monotonicity failed for " + gen.description +
             ": reduced=" +
             std::to_string(search.GetMemo().ExpressionCount(root)) +
             " full=" + std::to_string(explored_root_count);
    }
  }

  return "";
}

}  // namespace tinylamb
