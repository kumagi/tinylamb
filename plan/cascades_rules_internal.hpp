/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include "plan/cascades.hpp"

namespace tinylamb::cascades::rule_detail {

void InsertColumnMatcherNames(std::unordered_set<std::string>& out,
                              const ColumnName& col);

std::string TargetListFingerprint(const std::vector<NamedExpression>& targets);

std::string GroupIdsTag(std::initializer_list<GroupId> ids);

std::string GroupIdsTag(const std::vector<GroupId>& ids);

std::shared_ptr<AggregateExpression> CloneAggregate(
    const AggregateExpression& source, bool distinct);

bool TreeContainsWindowCall(const Expression& expression);

Expression CanonicalizeConjuncts(const Expression& predicate);

bool SelectionMayStrengthenSharedScans(const Memo& memo,
                                       GroupId selection_group);

void InheritBranchLocal(Memo& memo, GroupId derived, GroupId source);

bool ContainsAggregate(const Expression& expression);

bool ColumnsProvenDifferentType(const Memo& memo, const ColumnName& left,
                                const ColumnName& right);

bool IsSameTable(const std::string& t1, const std::string& t2);

bool ColumnIsDeclaredNonNull(const Memo& memo, const std::string& relation,
                             const ColumnName& column);

void InferJoinInequalities(Memo& memo, const Expression& join_predicate);

std::string ExpressionIdentity(const Expression& expression);

bool IsMultiplicityInsensitiveAggregate(const AggregateExpression& agg);

Expression StripRedundantDistinctInExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, bool* changed);

void CollectAggregatesAndValidate(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::vector<const AggregateExpression*>* out,
    bool* ok);

void CollectNonAggregateColumns(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::vector<ColumnName>* cols, bool* ok);

std::optional<Expression> ResolveHavingThroughOutputs(
    const Expression& expr, const std::vector<NamedExpression>& outputs);

bool HavingConjunctRejectsUnmatchedGroup(const Expression& resolved_conjunct);

std::optional<ValueType> ResolveMemoColumnType(const Memo& memo,
                                               const ColumnName& col);

std::string PredicateTag(const std::optional<Expression>& predicate);

void RegisterPredicateAndJoinRules(RuleSet& built);
void RegisterAggregateAndDistinctRules(RuleSet& built);

}  // namespace tinylamb::cascades::rule_detail
