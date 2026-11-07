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

// Returns true when a recursive CTE can be lifted to an opaque memo leaf.
bool RecursiveCteLiftable(const SelectStatement& outer,
                          const std::string& name);

}  // namespace tinylamb

#endif  // TINYLAMB_SELECT_REWRITES_HPP
