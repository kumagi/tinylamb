/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_FAST_PATHS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_FAST_PATHS_HPP

#include <optional>

#include "executor/detail/subquery_runtime.hpp"

namespace tinylamb::relational_detail {

struct ExactIntegerGroupingSets;

StatusOr<std::optional<Relation>> TryExecutePartitionBandedOrderByLimit(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes, bool columnar_output);

StatusOr<std::optional<Relation>> TryExecuteLexicographicDimPrefixTopK(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes, bool columnar_output);

StatusOr<std::optional<Relation>> TryExecuteColumnarNullMaskScalarCounts(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes);

StatusOr<std::optional<Relation>> TryExecuteRadixPacked2x32JoinCount(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes);

StatusOr<std::optional<Relation>> TryExecutePostingBandJoinScalarAgg(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes);

StatusOr<std::optional<Relation>> TrySurrogatePreAggExactGroupingInput(
    TransactionContext& context, const SelectStatement& head,
    const ExactIntegerGroupingSets& partial, const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecuteMultiBucketScalarCrossJoin(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecutePromoReturnThresholdCounts(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecuteMarketShareYearRoutes(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecuteOrderedFetchTopN(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecuteSurrogateIntersectCount(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecuteTwoPassGroupAvgThreshold(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes, bool columnar_output);

StatusOr<std::optional<Relation>> TryExecuteDirectionBuckets(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecuteRetireWitnessExists(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecuteRebucketLeftJoinHist(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryExecuteContiguousRevenueRuns(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes);

size_t FlattenableProjectionDepth(const SelectStatement& statement);

StatusOr<std::optional<Relation>> TryPushGroupedCteJoins(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes);

StatusOr<std::optional<Relation>> TryExecuteQuotientGidDistinctSetOpCount(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes);

StatusOr<std::optional<std::vector<Relation>>> TryExecuteBranchesParallel(
    TransactionContext& context,
    std::span<const SelectStatement* const> branches, const CteMap& ctes,
    bool columnar_output = false);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_FAST_PATHS_HPP
