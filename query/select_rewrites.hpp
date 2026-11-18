/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_SELECT_REWRITES_HPP
#define TINYLAMB_SELECT_REWRITES_HPP

#include <cstddef>
#include <string>
#include <unordered_set>
#include <vector>

#include "database/transaction_context.hpp"
#include "expression/named_expression.hpp"
#include "query/query_data.hpp"
#include "query/statement.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// M4 CTE inlining: replaces singly-referenced non-recursive CTEs with their
// defining query at the reference site when sound.
bool InlineSingleUseCtes(SelectStatement* outer);

// Inline single-use grouped bodies after pushing safe consumer restrictions,
// so their owned results flow directly into the join without a shared copy.
bool InlineSingleUseGroupedCtes(SelectStatement* outer,
                                TransactionContext& context);

// Shared-CTE restriction pushdown: pushes the disjunction of all consumer
// restrictions into a multi-referenced CTE body when selective.
size_t PushSharedCteRestrictions(SelectStatement* outer,
                                 TransactionContext& ctx);

// Expands one star select item carrying EXCEPT / REPLACE modifiers over one
// target schema.
void ExpandStarWithModifiers(const NamedExpression& item,
                             const std::string& relation, const Schema& schema,
                             std::vector<NamedExpression>* out,
                             std::unordered_set<std::string>* resolved_except,
                             std::unordered_set<std::string>* resolved_replace);

// M-union: merges compatible UNION ALL / UNION DISTINCT branches over one
// shared table into a single OR-filtered scan.
bool MergeCompatibleUnionBranches(SelectStatement* outer,
                                  TransactionContext& ctx);

// LATERAL decorrelation: converts single-table equality-correlated LATERAL
// sources into decorrelated joins.
bool DecorrelateSingleTableLaterals(SelectStatement* outer,
                                    TransactionContext& ctx);

// Sibling derived-aggregate fusion (e.g. TPC-H Q21): fuses sibling derived
// tables over the same base table and GROUP BY keys into a single aggregation.
size_t FuseSiblingDerivedAggregates(SelectStatement* outer);

// M5 derived-table flattening: inlines single-table (or grouped inner-join
// core) FROM-subqueries into the outer query when row-multiset preserving.
bool FlattenDerivedSources(SelectStatement* outer, TransactionContext& ctx,
                           const LiftedCtes* lifted = nullptr);

// CTE predicate pushdown: narrows a CTE body with the sound weakening of
// outer WHERE bounds across all reference sites.
bool TryNarrowCteBody(SelectStatement* body, const SelectStatement& outer,
                      const std::vector<std::string>& site_aliases);

// Functional-dependency GROUP BY reduction: drops non-PK grouping columns
// when the relation's primary key is already in the GROUP BY list.
bool ReduceGroupByFunctionalDependencies(SelectStatement* select,
                                         TransactionContext& ctx);

// Strips redundant DISTINCT on idempotent aggregates (MIN, MAX, LOGICAL_AND,
// LOGICAL_OR, BIT_AND, BIT_OR) without group-context modifiers (Lean 3-1).
bool StripIdempotentDistinctAggregates(SelectStatement* select);

// Pushes HAVING MAX(col) > c / >= c and MIN(col) < c / <= c bounds into WHERE
// when that single extremum is the only aggregate in the query and `col` has a
// non-DOUBLE reflexive domain, keeping HAVING intact (Lean Theorems 4-2/4-2').
bool PushHavingExtremumBounds(SelectStatement* select, TransactionContext& ctx);

// Drops unused LEFT JOIN sources without requiring UNIQUE/PK when the query's
// output is multiplicity-insensitive (SELECT DISTINCT without aggregates, or
// solely multiplicity-insensitive aggregates; Lean Theorem 3-4).
bool EliminateUnusedOuterJoinsForIdempotentResult(SelectStatement* select,
                                                  TransactionContext& ctx);

// Pulls INNER/CROSS joins ahead of preceding LEFT JOINs when the inner join's
// ON predicate does not reference the LEFT JOIN's null-supplying relation
// (Lean: TpcdsDiscovery.lean outer_join_intervening_inner_to_antijoin_sound).
bool PullInnerJoinsBeforeLeftJoins(SelectStatement* select,
                                   TransactionContext& ctx);

// Pushes selective dimension joins, WHERE filters, and partial GROUP BY into
// branches of a UNION ALL derived table (including zero-padded sales/returns
// unions; Lean: TpcdsDiscovery.lean zero_padded_union_branch_preagg_sound).
bool DistributeZeroPaddedUnionGroupBy(SelectStatement* select,
                                      TransactionContext& ctx);

// Splits tag-discriminated UNION ALL CTEs per channel, pushes disjunctive
// group-key bounds, and pivots multi-year self-joins into single-pass
// conditional aggregates (Lean: TpcdsDiscovery.lean
// union_cte_disjunctive_pushdown_and_branch_prune_sound,
// grouped_self_join_conditional_pivot_sound).
// Literal-specialized: caller must disarm compiled-plan caching when true.
bool SpecializeAndPivotUnionCtes(SelectStatement* outer,
                                 TransactionContext& ctx);

// Rewrites FULL OUTER JOIN indicator SUM(CASE ...) queries over duplicate-free
// (GROUP BY / SELECT DISTINCT) inputs into inclusion-exclusion counts without
// FULL OUTER JOIN (Lean: TpcdsDiscovery.lean
// full_outer_indicator_inclusion_exclusion_sound).
bool RewriteFullOuterIndicatorSumsToSemiJoin(SelectStatement* select);

// Eliminates self-joins between a derived/CTE scan and a grouped aggregate over
// the same source by computing the group aggregate as a partitioned window
// function with an explicit IS NOT NULL key guard (Lean: TpcdsDiscovery.lean
// self_join_group_agg_to_window_sound, self_join_agg_window_null_key_bug).
bool RewriteSelfJoinAggregateToWindow(SelectStatement* outer);

// Hoists duplicate expensive uncorrelated derived subqueries into a shared CTE
// so MaterializeCtes evaluates the subquery once (e.g. TPC-DS Q44 V1/V2).
bool HoistIdenticalDerivedSubqueriesToCte(SelectStatement* outer);

// Distributes GROUP BY (and Top-N) directly into UNION ALL branches without
// outer re-aggregation when a grouping column projects pairwise-distinct
// non-NULL constants across branches (Lean: TpcdsDiscovery.lean
// disjoint_union_group_by_bypass_sound, agg_union_null_tag_collision_bug).
// Literal-specialized: caller must disarm compiled-plan caching when true.
bool BypassDisjointUnionGroupBy(SelectStatement* select);

// Drops redundant constant and duplicate deterministic GROUP BY keys whenever
// at least one non-constant grouping key remains (e.g. ClickBench Q35
// `GROUP BY 1, URL` -> `GROUP BY URL`; Lean: ClickBenchDiscovery.lean
// group_by_functional_expr_reduction_sound).
bool PruneRedundantGroupByKeys(SelectStatement* select);

// Returns true when a recursive CTE can be lifted to an opaque memo leaf.
bool RecursiveCteLiftable(const SelectStatement& outer,
                          const std::string& name);

}  // namespace tinylamb

#endif  // TINYLAMB_SELECT_REWRITES_HPP
