/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/string_util.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "plan/cascades_rules_internal.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::cascades::rule_detail {
void RegisterAggregateAndDistinctRules(RuleSet& built) {
  using namespace dsl;
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
            expression.children.size() != 1 || expression.target_list.empty()) {
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
          updated_pred = StripRedundantDistinctInExpr(*updated_pred, &changed);
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
            expression.children.size() != 1 || expression.target_list.empty()) {
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
        const std::vector<NamedExpression> sel_targets = expression.target_list;
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
              ResolveHavingThroughOutputs(having_pred, aggregation.target_list);
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
                       agg->Child()->AsColumnValue().GetColumnName() == ext_col;
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
            const bool is_max_lower_bound =
                ext_type == AggregationType::kMax &&
                (op == BinaryOperation::kGreaterThan ||
                 op == BinaryOperation::kGreaterThanEquals);
            const bool is_min_upper_bound =
                ext_type == AggregationType::kMin &&
                (op == BinaryOperation::kLessThan ||
                 op == BinaryOperation::kLessThanEquals);
            if (is_max_lower_bound || is_min_upper_bound) {
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
          if (filtered == group || filtered == agg_id || filtered == input_id) {
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
              group, LogicalExpression{.operation = LogicalOperator::kSelection,
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
        const std::vector<NamedExpression> sel_targets = expression.target_list;
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
              ResolveHavingThroughOutputs(having_pred, aggregation.target_list);
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
            const bool all_aggs_strict_on_right =
                std::ranges::all_of(aggs, [&](const AggregateExpression* agg) {
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
  // JoinPreaggregation.lean & AvgMinMaxHavingDistinct.lean): Push a 2-stage
  // partial Aggregation below an inner equi-join Join(L, R, equi_pred) onto
  // side S when all aggregates (COUNT, MIN, MAX) depend exclusively on S
  // and GROUP BY is non-empty. Pre-aggregates S by (outer_keys_in_S ∪
  // join_keys_in_S), joins the collapsed table with the other side, and
  // finishes with final SUM/MIN/MAX above the join (COUNT -> SUM(partial)).
  // Cascades across multi-hop star/snowflake joins automatically.
  // Lean proof: lean/TinylambOpt/JoinPreaggregation.lean
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
            expression.children.size() != 1 || expression.target_list.empty() ||
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
          const std::vector<std::string> left_vec = memo.Get(left_id).relations;
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
              pre_targets.emplace_back(synth_name, CloneAggregate(orig, false));
              const AggregationType final_op =
                  orig.GetType() == AggregationType::kCount
                      ? AggregationType::kSum
                      : orig.GetType();
              auto final_agg = std::make_shared<AggregateExpression>(
                  final_op, ColumnValueExp(ColumnName("", synth_name)), false);
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
                nn_filters.push_back(UnaryExpressionExp(
                    ColumnValueExp(side_join_col), UnaryOperation::kIsNotNull));
              }
            }
            if (!nn_filters.empty()) {
              const Expression nn_pred =
                  CanonicalizeConjuncts(CombineConjuncts(nn_filters));
              const GroupId nn_group = memo.EnsureDerivedGroup(
                  side_relations, "eager_nn_filter:" + GroupIdsTag({side_id}) +
                                      nn_pred->ToString());
              InheritBranchLocal(memo, nn_group, group);
              if (nn_group != group && nn_group != side_id) {
                memo.AddExpression(
                    nn_group,
                    LogicalExpression{.operation = LogicalOperator::kSelection,
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
                side_relations, "eager_pre_agg:" + GroupIdsTag({pre_input_id}) +
                                    pre_key_tag +
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
  // Lean proof: lean/TinylambOpt/UnionAndJoinRewrites.lean
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
              const Value& val = item.expression->AsConstantValue().GetValue();
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
                branch_rels, "disjoint_union_agg_branch_" + std::to_string(i) +
                                 ":" + std::to_string(branch_id) + ":" + fp);
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
              group, LogicalExpression{.operation = LogicalOperator::kUnionAll,
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
  // Lean proof: lean/TinylambOpt/UnionAndJoinRewrites.lean
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
             memo.Get(input_id).tag.starts_with("outer_to_anti_base_thru:"))) {
          return;
        }
        const std::vector<LogicalExpression> join_c_candidates =
            memo.Get(input_id).expressions;
        const std::vector<std::string> input_relations =
            memo.Get(input_id).relations;
        const Expression sel_predicate = *expression.predicate;
        const std::vector<NamedExpression> sel_targets = expression.target_list;
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
                      return !c.schema.empty() && ac_rels.contains(c.schema) &&
                             !b_rels.contains(c.schema);
                    })) {
                  continue;
                }
              }

              size_t null_checks = 0;
              bool convertible = true;
              std::vector<Expression> ac_residuals;
              for (const Expression& conjunct : SplitConjuncts(sel_predicate)) {
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
                      return !c.schema.empty() && ac_rels.contains(c.schema) &&
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
                    group,
                    LogicalExpression{.operation = LogicalOperator::kSelection,
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
  // Lean proof: lean/TinylambOpt/UnionAndJoinRewrites.lean
  //   (self_join_group_agg_to_window_sound,
  //    self_join_agg_window_null_key_bug)
  built.Add(Rule(
      "self_join_agg_to_window", Projection(Any("input")),
      [](const Bindings& bindings, Memo& memo, GroupId group,
         const LogicalExpression& expression) {
        if (expression.operation != LogicalOperator::kProjection ||
            expression.children.size() != 1 || expression.target_list.empty()) {
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
          return changed ? WithExpressionChildren(expr, std::move(kids)) : expr;
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
            std::optional<Expression> mk = self(self, k, s_rel, b_rel, key_name,
                                                agg_out_name, win_col_name);
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
                const auto& expressions = memo.Get(scan_group).expressions;
                return std::ranges::any_of(
                    expressions, [&](const LogicalExpression& e) {
                      if (e.operation != LogicalOperator::kScan ||
                          e.table.empty()) {
                        return false;
                      }
                      if (e.table == shared_table) {
                        return true;
                      }
                      const auto owner = rel_schemas.find(e.table);
                      return owner != rel_schemas.end() &&
                             owner->second.Name() == shared_table;
                    });
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
                const ColumnName& gk_col = gk->AsColumnValue().GetColumnName();
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
                    if (!a.Child() || !SafeToReduceEvaluationCount(a.Child())) {
                      targets_valid = false;
                      break;
                    }
                    const auto a_cols = a.Child()->TouchedColumns();
                    if (!std::ranges::all_of(a_cols, [&](const ColumnName& c) {
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
                all_conjuncts.insert(all_conjuncts.end(), sel_conjuncts.begin(),
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
                    out_rels, "self_join_win_sel:" + GroupIdsTag({win_group}) +
                                  new_sel_pred->ToString());
                InheritBranchLocal(memo, new_sel_group, group);
                if (new_sel_group == group || new_sel_group == win_group ||
                    new_sel_group == win_base) {
                  continue;
                }
                memo.AddExpression(
                    new_sel_group,
                    LogicalExpression{.operation = LogicalOperator::kSelection,
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
  // Lean proof: lean/TinylambOpt/UnionAndJoinRewrites.lean
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
              proj_expr.children.size() != 1 || proj_expr.target_list.empty()) {
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
              const std::unordered_set<std::string> d2_rels(d2_rels_vec.begin(),
                                                            d2_rels_vec.end());

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
                          std::ranges::find(fact_keys, c) == fact_keys.end()) {
                        fact_keys.push_back(c);
                      }
                    }
                  }
                  if (!preds_ok || fact_keys.empty() || fact_keys.size() > 4) {
                    continue;
                  }
                  std::ranges::sort(
                      fact_keys, [](const ColumnName& a, const ColumnName& b) {
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
                  if (fact_proj_group == group || fact_proj_group == fact_id) {
                    continue;
                  }
                  memo.AddExpression(
                      fact_proj_group,
                      LogicalExpression{
                          .operation = LogicalOperator::kProjection,
                          .children = {fact_id},
                          .target_list = std::move(fact_proj_targets)});

                  const GroupId fact_dist_group = memo.EnsureDerivedGroup(
                      fact_rels_vec,
                      "eager_distinct_fact:" + GroupIdsTag({fact_proj_group}) +
                          fk_tag);
                  InheritBranchLocal(memo, fact_dist_group, group);
                  if (fact_dist_group == group ||
                      fact_dist_group == fact_proj_group) {
                    continue;
                  }
                  memo.AddExpression(
                      fact_dist_group,
                      LogicalExpression{.operation = LogicalOperator::kDistinct,
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
                      LogicalExpression{.operation = LogicalOperator::kJoin,
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
                      LogicalExpression{.operation = LogicalOperator::kJoin,
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
}
}  // namespace tinylamb::cascades::rule_detail
