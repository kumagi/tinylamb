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
// (Lean: UnionAndJoinRewrites.lean
// outer_join_intervening_inner_to_antijoin_sound).
bool PullInnerJoinsBeforeLeftJoins(SelectStatement* select,
                                   TransactionContext& ctx);

// Pushes selective dimension joins, WHERE filters, and partial GROUP BY into
// branches of a UNION ALL derived table (including zero-padded sales/returns
// unions; Lean: UnionAndJoinRewrites.lean
// zero_padded_union_branch_preagg_sound).
bool DistributeZeroPaddedUnionGroupBy(SelectStatement* select,
                                      TransactionContext& ctx);

// Splits tag-discriminated UNION ALL CTEs per channel, pushes disjunctive
// group-key bounds, and pivots multi-year self-joins into single-pass
// conditional aggregates (Lean: UnionAndJoinRewrites.lean
// union_cte_disjunctive_pushdown_and_branch_prune_sound,
// grouped_self_join_conditional_pivot_sound).
// Literal-specialized: caller must disarm compiled-plan caching when true.
bool SpecializeAndPivotUnionCtes(SelectStatement* outer,
                                 TransactionContext& ctx);

// Rewrites FULL OUTER JOIN indicator SUM(CASE ...) queries over duplicate-free
// (GROUP BY / SELECT DISTINCT) inputs into inclusion-exclusion counts without
// FULL OUTER JOIN (Lean: UnionAndJoinRewrites.lean
// full_outer_indicator_inclusion_exclusion_sound).
bool RewriteFullOuterIndicatorSumsToSemiJoin(SelectStatement* select);

// Eliminates self-joins between a derived/CTE scan and a grouped aggregate over
// the same source by computing the group aggregate as a partitioned window
// function with an explicit IS NOT NULL key guard (Lean:
// UnionAndJoinRewrites.lean self_join_group_agg_to_window_sound,
// self_join_agg_window_null_key_bug).
bool RewriteSelfJoinAggregateToWindow(SelectStatement* outer);

// Pushes partition-only QUALIFY conjuncts below window computation into WHERE
// when all window functions share the referenced partition columns (and GROUP
// BY keys if grouped). Lean proof:
// lean/TinylambOpt/ProposedProofs/QualifyPushdown.lean (sound)
bool PushPartitionOnlyQualifyToWhere(SelectStatement* select);

// Rewrites grouped `COUNT(*)` over two-table FULL OUTER / LEFT / INNER
// equi-joins into a join of pre-aggregated per-key frequency histograms
// `(k, COUNT(*) AS __cnt)`.
// Lean proof: lean/TinylambOpt/ResearchProofs/OuterJoinCounts.lean (sound)
bool RewriteOuterJoinGroupedCountToHistograms(SelectStatement* select,
                                              TransactionContext& ctx);

// Hoists duplicate expensive uncorrelated derived subqueries into a shared CTE
// so MaterializeCtes evaluates the subquery once (e.g. TPC-DS Q44 V1/V2).
bool HoistIdenticalDerivedSubqueriesToCte(SelectStatement* outer);

// Distributes GROUP BY (and Top-N) directly into UNION ALL branches without
// outer re-aggregation when a grouping column projects pairwise-distinct
// non-NULL constants across branches (Lean: UnionAndJoinRewrites.lean
// disjoint_union_group_by_bypass_sound, agg_union_null_tag_collision_bug).
// Literal-specialized: caller must disarm compiled-plan caching when true.
bool BypassDisjointUnionGroupBy(SelectStatement* select);

// Drops redundant constant and duplicate deterministic GROUP BY keys whenever
// at least one non-constant grouping key remains (e.g. ClickBench Q35
// `GROUP BY 1, URL` -> `GROUP BY URL`; Lean: GroupKeyAndDictionaryRewrites.lean
// group_by_functional_expr_reduction_sound).
bool PruneRedundantGroupByKeys(SelectStatement* select);

// Folds `NOT (x IS [NOT] NULL/TRUE/FALSE)` into its exact 3VL dual unary
// predicate (Lean: PredicateAndRelationalLowering.lean
// two_valued_predicate_not_involution_sound, sqlIsNull_never_tNull).
bool CanonicalizeTwoValuedNegations(SelectStatement* select);

// Canonicalizes unqualified GROUP BY items that either resolve to a single
// base relation while shadowed by a SELECT alias, or reference an unshadowed
// deterministic SELECT alias (Lean: PredicateAndRelationalLowering.lean
// select_alias_group_by_canonicalization_sound).
bool CanonicalizeSelectAliasGroupBy(SelectStatement* select,
                                    TransactionContext& ctx);

// Pushes the disjunction of consumer WHERE constant restrictions on a GROUP BY
// key into a multi-referenced grouped CTE body and any inner UNION branches
// (Lean: PredicateAndRelationalLowering.lean
// shared_grouped_union_distinct_cte_disjunctive_pushdown_sound).
// Literal-specialized: caller must disarm compiled-plan caching when true.
bool PushSharedGroupedCteKeyRestrictions(SelectStatement* outer,
                                         TransactionContext& ctx);

// Pre-aggregates surrogate join keys `(fk, ...)` via GROUP BY before joining a
// large VARCHAR dimension inside SELECT DISTINCT set-operation branches
// (Lean: PredicateAndRelationalLowering.lean
// eager_distinct_surrogate_keys_setop_sound,
// except_below_non_injective_dim_unsound).
bool PushDistinctSurrogateKeysBeforeDimJoins(SelectStatement* outer,
                                             TransactionContext& ctx);

// Factors common conjuncts out of N-ary ORs, synthesizes per-table disjunctive
// envelopes when every branch constrains that table, and appends SARGable
// numeric/date interval hulls (Lean: JoinAndAggregatePushdown.lean
// kleene_common_conjunct_factor_or_sound,
// disjunctive_join_envelope_pushdown_sound, interval_hull_of_disjuncts_sound,
// envelope_missing_branch_pred_drops_rows_bug).
bool FactorAndEnvelopeDisjunctiveWhere(SelectStatement* select,
                                       TransactionContext& ctx);

// Pre-aggregates the null-supplying right table of a LEFT OUTER JOIN for
// null-strict COUNT(colR) aggregates, eliding the outer GROUP BY only when the
// left key is certified UNIQUE + NOT NULL (Lean: JoinAndAggregatePushdown.lean
// left_join_right_preagg_and_unique_elide_groupby_sound,
// nullable_unique_left_elides_groupby_bug, left_join_count_nonstrict_expr_bug).
bool PreaggregateLeftJoinRightCounts(SelectStatement* select,
                                     TransactionContext& ctx);

// Absorbs `key IN (SELECT key FROM T WHERE w GROUP BY key HAVING h)` together
// with an outer join to the same base table T (under matching WHERE w) into a
// single grouped derived table over T (Lean: JoinAndAggregatePushdown.lean
// self_semijoin_having_agg_absorption_sound,
// self_semijoin_mismatched_where_bug).
bool AbsorbSelfSemijoinHavingAggregate(SelectStatement* select,
                                       TransactionContext& ctx);

// Rewrites a doubly-referenced CTE filtered by `val = (SELECT MAX/MIN(val)
// FROM cte)` into a singly-referenced `WHERE val IS NOT NULL ORDER BY val
// DESC/ASC FETCH FIRST 1 ROWS WITH TIES` CTE (Lean:
// JoinAndAggregatePushdown.lean cte_argmax_subquery_to_nonnull_max_ties_sound,
// argmax_with_ties_without_is_not_null_bug).
bool RewriteCteArgmaxSubqueryToTopTies(SelectStatement* outer,
                                       TransactionContext& ctx);

// Synthesizes missing pairwise equijoins across 3+-relation equality classes
// so dimensions can join before large fact tables (Lean:
// JoinAndAggregatePushdown.lean equijoin_transitive_closure_sound).
bool SynthesizeTransitiveEquijoins(SelectStatement* select,
                                   TransactionContext& ctx);

// Defers unreferenced wide/dependent attributes of certified unique dimensions
// past `GROUP BY ... ORDER BY agg LIMIT K` while retaining foreign-key
// semi-join filtering inside the Top-K core (Lean:
// JoinAndAggregatePushdown.lean topn_agg_late_unique_dim_attribute_join_sound,
// late_dim_join_without_key_semijoin_steals_limit_bug).
bool DeferUniqueDimensionAttributesAfterTopN(SelectStatement* select,
                                             TransactionContext& ctx);

// Decorrelates strict comparisons against correlated `SUM`/`AVG`/`MIN`/`MAX`
// scalar subqueries into INNER JOINs over grouped derived tables, strictly
// rejecting `COUNT` and `COALESCE` (Lean: JoinAndAggregatePushdown.lean
// correlated_gt_sum_decorrelate_to_inner_join_sound,
// correlated_gt_count_demotes_to_inner_join_bug).
bool DecorrelateCorrelatedSumComparisons(SelectStatement* select,
                                         TransactionContext& ctx);

// Factors common conjuncts inside correlated scalar `COUNT(*)` subqueries and
// rewrites top-level WHERE `(SELECT COUNT(*) FROM S WHERE inner_k = outer_k
// AND P) > 0` (and `>= 1`) into `outer_k IN (SELECT inner_k FROM S WHERE P AND
// inner_k IS NOT NULL)` (Lean: PredicateAndRelationalLowering.lean
// kleene_and_or_distrib_left_sound,
// correlated_count_gt_zero_to_in_subquery_sound,
// trap_correlated_count_eq_zero_not_in_null_outer_unsound).
bool RewriteCorrelatedPositiveCountToInSubquery(SelectStatement* select,
                                                TransactionContext& ctx);

// Rewrites `HAVING COUNT(DISTINCT x) > 1` (or `>= 2`) to `MIN(x) < MAX(x)` and
// `HAVING COUNT(DISTINCT x) = 1` to `MIN(x) = MAX(x)` for non-NaN totally
// ordered columns (`INT64`, `DATE`, binary `VARCHAR`).
// Lean: VectorizedJoinAndPruning.lean
//   (count_distinct_gt_one_iff_min_lt_max,
//    count_distinct_eq_one_iff_min_eq_max).
bool RewriteHavingCountDistinctToMinMax(SelectStatement* select,
                                        TransactionContext& ctx);

// Rewrites `GROUP BY k HAVING SUM(v) op <expr(SELECT SUM(v) FROM same_join)>`
// into a single-pass CTE using Fubini's theorem (`SUM(group_sum) = SUM(v)`).
// Rejects `SUM(DISTINCT v)` and filtered sums.
// Lean: JoinAndAggregatePushdown.lean
//   (grouped_having_global_sum_fubini_sound,
//    grouped_having_distinct_sum_fubini_bug).
bool RewriteHavingGlobalSumFubini(SelectStatement* stmt,
                                  TransactionContext& ctx);

// Rewrites `FROM F, D WHERE D.pk = F.fk AND <D preds> AND F.c op (SELECT
// <expr(AGG(F2.c2))> FROM F F2 WHERE F2.fk = D.pk)` (when `D.pk` is certified
// UNIQUE NOT NULL) into a single-scan shared CTE over `F ⋈ D`.
// Lean: VectorizedJoinAndPruning.lean
//   (self_join_unique_dim_cte_decorrelate_sound).
bool RewriteSelfJoinUniqueDimCorrelatedAgg(SelectStatement* stmt,
                                           TransactionContext& ctx);

// Pushes the filtered outer key domain into a heavy `k IN (SELECT r_k FROM
// ...)` subquery (and transitively into any correlated subquery equated with
// `r_k`), materializing the filtered outer domain once as a CTE. Lean:
// VectorizedJoinAndPruning.lean
//   (semijoin_domain_pushdown_in_subquery_sound).
bool RewritePushOuterSemiJoinDomainIntoInSubquery(SelectStatement* stmt,
                                                  TransactionContext& ctx);

// Pushes the intersected key domain `(L_keys) ∩ (R1_keys ∪ R2_keys)` into
// grouped CTEs `L`, `R1`, `R2` when `L LEFT JOIN R1 LEFT JOIN R2` has a
// disjunctive filter that rejects double-unmatched rows (e.g.
// `COALESCE(r1.qty, 0) > 0 OR COALESCE(r2.qty, 0) > 0`, TPC-DS Q78).
// Lean: VectorizedJoinAndPruning.lean
//   (disjunctive_left_join_semijoin_pushdown_sound).
bool RewriteDisjunctiveLeftJoinCteKeyDomain(SelectStatement* stmt,
                                            TransactionContext& ctx);
bool RewriteDistributeJoinOverUnionAll(SelectStatement* stmt,
                                       TransactionContext& ctx);
bool RewriteDistributeGroupedSumOverUnionAll(SelectStatement* root,
                                             TransactionContext& ctx);
bool PushTransitiveGroupedSubqueryDimSemiFilter(SelectStatement* root,
                                                TransactionContext& ctx);
bool RewriteCorrelatedSelfAvgToWindow(SelectStatement* root,
                                      TransactionContext& ctx);

// Merges a cross-join of two ungrouped scalar-aggregate derived tables over
// the same base join (either with differing local predicates, or where one
// side adds a single certified-unique-PK dimension table) into a single-pass
// conditional-aggregate derived table.
bool RewriteScalarAggCrossJoinMerge(SelectStatement* root,
                                    TransactionContext& ctx);

// Decomposes `SUM(col ± c)` into `SUM(col) + (±c * COUNT(col))` when signed
// INT64 column bounds certify zero per-row or cumulative overflow, preserving
// 3VL NULL on empty/all-NULL groups (Lean: CountBoundsAndBlockAggregates.lean
// affine_sum_homomorphism_sound, affine_sum_coalesce_empty_bug,
// affine_sum_overflow_masked_bug).
bool RewriteAffineLinearSums(SelectStatement* select, TransactionContext& ctx);

// Defers joins to runtime-verified duplicate-free non-null dimensions past
// GROUP BY when a unique key of the dimension determines its attributes and no
// aggregate argument reads the dimension (Lean:
// PredicateAndRelationalLowering.lean bijective_dim_key_cte_deferral_sound).
// Literal/snapshot-specialized.
bool DeferUniqueDimJoinPastGroupBy(SelectStatement* outer,
                                   TransactionContext& ctx);

// Replaces unreferenced INNER JOINs to runtime-verified contiguous non-null
// unique integer PK tables `fk = D.pk` with `fk >= min_pk AND fk <= max_pk`
// (Lean: PredicateAndRelationalLowering.lean
// unreferenced_unique_pk_inner_join_to_semi_sound). Snapshot-specialized:
// caller must disarm compiled-plan caching when true.
bool EliminateUnreferencedUniquePkInnerJoins(SelectStatement* outer,
                                             TransactionContext& ctx);

// Eliminates CROSS/INNER joins to guaranteed-singleton scalar aggregate CTEs
// or derived tables inside plain GROUP BY queries when referenced only via
// idempotent MIN/MAX aggregates, restoring a scalar subquery (Lean:
// PredicateAndRelationalLowering.lean singleton_scalar_cross_join_elim_sound,
// trap_singleton_cross_join_sum_not_idempotent).
bool EliminateSingletonScalarCrossJoins(SelectStatement* outer,
                                        TransactionContext& ctx);

// Pushes SELECT DISTINCT + selective inner-joined dimension filters into
// branches of a UNION ALL derived table before joining remaining outer tables
// (Lean: PredicateAndRelationalLowering.lean
// distinct_union_all_dim_distribution_sound).
bool DistributeDistinctUnionDimJoins(SelectStatement* outer,
                                     TransactionContext& ctx);

// Rewrites 3-way `(v1, v1_lag, v1_lead)` `rn = rn_lag + 1 AND rn = rn_lead - 1`
// self-joins over grouped `RANK()` CTEs into single-pass `LAG`/`LEAD` window
// functions (Lean: PredicateAndRelationalLowering.lean
// adjacent_rank_self_join_to_lag_lead_sound).
bool RewriteAdjacentRankSelfJoinToLagLead(SelectStatement* outer);

// Folds `RANK()`/`DENSE_RANK()`/`ROW_NUMBER()` to constant `1` when its
// PARTITION BY covers the query's GROUP BY keys, and absorbs tautological
// self-domain `IN (SELECT key FROM same_join GROUP BY key)` filters into
// `key IS NOT NULL` (Lean: PredicateAndRelationalLowering.lean
// singleton_partition_rank_const_one_sound).
bool FoldSingletonPartitionRankToOne(SelectStatement* outer);

// Collapses N >= 2 deferred unique-dimension joins (`__tpcds_dim_pre ⋈ D` on
// `fk = D.pk`) across inner-joined derived subqueries into a single post-join
// dimension lookup when the outer query equates all N subqueries on a unique
// non-null attribute `D.uk` (Lean: PredicateAndRelationalLowering.lean
// multi_join_shared_unique_dim_deferral_sound,
// trap_non_unique_uk_dim_collapse_unsound). Snapshot-specialized.
bool CollapseSharedUniqueDimAcrossJoinedSubqueries(SelectStatement* outer,
                                                   TransactionContext& ctx);

// Pushes a disjunctive null-rejecting outer WHERE filter `(p_1(r_1) OR ... OR
// p_k(r_k))` over LEFT JOINed grouped CTEs `r_1..r_k` as an `EXISTS (...) OR
// ...` semi-filter into the preserved grouped CTE `s_0`'s fact scan (plus
// constant group-key restrictions into `s_0, r_1..r_k`) before CTE
// materialization (Lean: PredicateAndRelationalLowering.lean
// left_join_disjunctive_null_rejecting_semi_filter_sound,
// trap_left_join_non_strict_predicate_semi_unsound). Literal-specialized.
bool PushDisjunctiveNullRejectingLeftJoinSemiFilter(SelectStatement* outer,
                                                    TransactionContext& ctx);

// Returns true when a recursive CTE can be lifted to an opaque memo leaf.
bool RecursiveCteLiftable(const SelectStatement& outer,
                          const std::string& name);

// Hoists structurally identical grouped derived-table sources (e.g. TPC-DS Q65
// `sa` inside `sb` and sibling `sc`) into a shared CTE so the fact scan and
// inner GROUP BY execute once instead of twice.
// Lean:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/PivotedAggregatesAndSemijoinFilters.lean
//   (GroupAvgThresholdTwoPassSplit_sound).
bool HoistDuplicateDerivedTablesToCte(SelectStatement* select);

// Pre-aggregates a single-table COUNT(*) query by its underlying base columns
// when GROUP BY contains an expensive VARCHAR CASE/LIKE/function expression
// over <= 3 base columns, then evaluates the expression over the compact
// grouped keys and folds counts with SUM (Public BI MLB suffix CASE).
// Lean: CloudBI/PublicBI.lean (sound_GroupBeforeSuffix).
bool PreGroupBeforeExpressionGroupBy(SelectStatement* select,
                                     TransactionContext& ctx);

// Collapses a single-table GROUP BY whose every grouping key is pinned to a
// type-matching non-NULL constant by WHERE equalities into an ungrouped scalar
// aggregate wrapped by `WHERE __cg_cnt > 0` (Public BI MLB Q108).
// Lean: CloudBI/PublicBI.lean (sound_ConstantGroup).
bool CollapseConstantGroupBy(SelectStatement* select, TransactionContext& ctx);

// Factors a 3-table `(X, F, E)` IN/NOT IN subquery with an expression equijoin
// between X and E and a predicate between F and E into `X JOIN (SELECT DISTINCT
// f_keys, expr_e FROM F JOIN E WHERE pred_fe)` so the small (F x E) delete-key
// table is built and deduplicated before hash-probing X (Iceberg equality).
// Lean: AnalyticalOperators/FilePruningAndDeleteMasks.lean
// (FileKeyAntiJoin_sound).
bool FactorDeleteAntiJoinSubquery(SelectStatement* select);

// Pre-aggregates a shared-scan ROLLUP / GROUPING SETS UNION ALL query by the
// surrogate foreign key before joining a runtime-verified unique non-null
// dimension table when all ROLLUP keys come from that dimension (TPC-DS Q22).
// Lean:
// lean/TinylambOpt/ResearchProofs/QueryRewrites/SurrogateKeyRollupsAndSetCounts.lean
// (SurrogateKeyPreAggRollup_sound).
bool PreAggregateSurrogateKeyBeforeRollup(SelectStatement* outer,
                                          TransactionContext& ctx);

// Flattens nested `SELECT SUM(col) FROM (... UNION ALL ...)` trees and folds
// structurally identical leaf `SELECT SUM(expr)` branches with multiplicity
// `m >= 2` into a single branch `SUM(expr) * m` (Snowflake BI / PBench).
// Lean: CloudBI/NestedBI.lean (sound_FlattenSum, sound_BranchHistogram).
bool FoldNestedSumUnionAllBranches(SelectStatement* select);

}  // namespace tinylamb

#endif  // TINYLAMB_SELECT_REWRITES_HPP
