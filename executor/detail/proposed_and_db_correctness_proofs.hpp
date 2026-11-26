/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_PROPOSED_AND_DB_CORRECTNESS_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_PROPOSED_AND_DB_CORRECTNESS_PROOFS_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// ProposedGoals (48 files -> 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q03_BlockFooterExactSumCountReuseGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q04_DisjointHashPartitionDistinctSumGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q09_PerBlockRegionDistinctMergeGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q11_DictNonEmptyCodeFilterDistinctGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q12_InjectivePairCodeDistinctCountGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q13_AllEmptyBlockSkipFrequencyGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q14_RowCountUpperBoundsDistinctTopKGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q15_LocalDictCodeCountTranslationGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q16_HeavyHitterCountLowerBoundPruningGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q17_EmptyPhraseFastPartitionSplitGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q18_UnorderedLimit10FixedKeySetCountGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q20_ZoneMapExactEqualityBlockSkipOrAcceptGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q22_CountTopKBeforeMinUrlLookupGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q23_DictCodeSubstringMaskEquivGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q24_EventTimeFenceBeforeUrlSubstringGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q25_LateSearchPhraseDecodeAfterTimeTopKGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q27_TimeTieBoundaryForSecondaryStringSortGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q28_DictCodeHostExtractAndHavingBoundGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q31_PackedEngineIpKeyBijectionGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q32_UniqueWatchIdSingletonGroupGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q36_DerivedIpColumnsAfterGroupingGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q37_CounterDateBlockPruneBeforeUrlCountGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q38_SparseRowPositionTitleGatherGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q39_OffsetWindowFromSortedFrequencyPrefixGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q40_DisjointReferrerCaseBranchSplitGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q42_RowCountBoundsDistinctOffsetEmptyGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/ClickBench/Goal_Q43_SingleMinuteBlockDirectBucketCountGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q49_PositiveReturnFilterInnerJoinReductionGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q53_ItemBitsetPrefilterBeforeQuarterWindowGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q57_DeviationBandFilterBeforeNeighborJoinGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q58_PairwiseRatioBandCrossMultiplicationGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q59_ShiftedWeekIndexLookupEquivGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q62_DelayBucketPartitionSumGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q63_DisjunctiveItemBitsetDeduplicationGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q66_WarehouseMonthPivotLateAttrLookupGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q71_UnionAllBranchPreaggAdditiveGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q72_LeftJoinMultiplicityPromoSplitGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q74_PositiveBaselineFilterBeforeTopKGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q75_ChannelLocalDedupBeforeUnionDistinctGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q79_FunctionalAddressKeyElisionGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q81_StateAvgFromCustomerTotalsGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q83_CommonItemTopKBeforeQtyLookupGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q85_EqualNonNullDemoCodeFilterGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q86_CompetitionRankFromHigherCountGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q87_CandidateOnlyDifferenceMarksGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q89_MonthlyDeviationBandExactCheckGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q90_SinglePassAmPmTimeBandCountsGoal.lean
// lean/TinylambOpt/ResearchProofs/ProposedGoals/TpcdsPart2/Goal_Q99_RunLengthDelayHistogramAccumGoal.lean
//
// DatabaseCorrectness (74 files -> 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/CatalogDrop_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/ChunkSlices_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/DurableReplies_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_CatalogDrop_AbortedCloseKeepsConcurrentReleases.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_CatalogDrop_ForeignReferenceDropInterlock.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_CatalogDrop_NoLeaseAdmissionAfterReferenceCheck.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_CatalogDrop_QuiescentDropDrainsExactMultiset.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_CatalogDrop_RecreatedNameRejectsOldLease.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_CatalogDrop_RepeatedTableLeaseBlocksReclaim.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_ChunkSlices_ConstantOrFlatSliceDecodesExactly.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_ChunkSlices_EmptyStringNullSliceBoundary.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_ChunkSlices_NestedChunkViewIdentity.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_ChunkSlices_NullSliceKeepsComparisonDomain.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_ChunkSlices_SpillPositionsStayWithSlicedRows.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_ChunkSlices_UnionAllCrossChunkLimitOffset.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_DurableReplies_AbortAfterDurabilityFailureIsIrrevocable.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_DurableReplies_AsyncCommitKeepsReadBarrier.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_DurableReplies_ChainedObservationBarrier.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_DurableReplies_EmptyWriterDependencyCut.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_DurableReplies_InlineAndDeferredBarrierAgreement.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_DurableReplies_PartialCommitRecordCannotAcknowledge.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_DurableReplies_TerminalReplyCannotBeUpgraded.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_DurableReplies_WriteReplyRequiresPublicationAndBarrier.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_MergeIntegrity_DeleteSetForeignKeyCriterion.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_MergeIntegrity_DuplicateSourceIdentityConflict.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_MergeIntegrity_FailedMergePhaseCannotPublishPrefix.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_MergeIntegrity_FirstMatchingMergeAction.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_MergeIntegrity_InsertPhaseOwnEarlierParent.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_MergeIntegrity_NoActionCannotBecomeInsert.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_MergeIntegrity_NullMergeAntiRowsRetainMultiplicity.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_MergeIntegrity_RekeyLastReferencedParent.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_RecoveryBoundary_EarliestUsableCheckpointAnchor.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_RecoveryBoundary_FooterEndCannotMaskLostCommit.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_RecoveryBoundary_ForcedRepairWithoutWriterFails.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_RecoveryBoundary_LaterValidFramePreventsTailTruncation.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_RecoveryBoundary_RecycledWalNeedsAnchor.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_RecoveryBoundary_ReopenClearsPriorRepairFlag.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_RecoveryBoundary_RepairFlagRequiresSuccessfulTruncate.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SimpleQuery_BeginAdoptsImplicitMessagePrefix.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SimpleQuery_DiscardUnconfirmedReplyAtWireCut.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SimpleQuery_ErrorAfterCommitRollsBackOnlyNewBlock.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SimpleQuery_FailedExplicitCommitDiscardsStaging.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SimpleQuery_ReadyBoundarySeparatesImplicitTransactions.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SimpleQuery_RollbackStartsFreshImplicitBlock.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SimpleQuery_WireCompactionPreservesEarlierPending.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_DiscardPartialChunkThenRetry.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_FailedUndoAllocationStillRetryable.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_MergeRetryLimitPrefix.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_RestoredWinnerRowRetry.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_RetainPartialChunkAcrossRowFault.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_RetriedMergePassRetainsStableStream.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_SingleRowCommitConservation.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_SpillTiesPreserveInputOccurrenceOrder.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_StatisticsCache_AbortDdlEpochRevalidatesSnapshot.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_StatisticsCache_CaseAliasInvalidationComplete.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_StatisticsCache_CatalogLookupFailureCanRetry.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_StatisticsCache_RenameInvalidationPreservesOtherTables.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_StatisticsCache_SameLiveEpochDifferentSnapshot.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_StatisticsCache_ThreadStatisticsRefinesCatalog.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/GroupingOutput_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/MergeIntegrity_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/NullableMembership_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/PinnedScan_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/Publication_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/RecoveryBoundary_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/SimpleQueryCommon.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/SimpleQuery_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/SnapshotIndex_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/SortRetryCommon.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/SortRetry_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/StatisticsCache_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/VarcharKeys_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/WalReread_All.lean
// lean/TinylambOpt/ResearchProofs/DatabaseCorrectness/WinnerRecovery_All.lean
// ============================================================================

// MVCC snapshot visibility predicate: version `[begin_ts, end_ts)` is visible
// to snapshot `read_ts` iff `begin_ts <= read_ts && read_ts < end_ts`.
[[nodiscard]] constexpr bool MvccVersionVisibleAtSnapshot(
    uint64_t begin_ts, uint64_t end_ts, uint64_t read_ts) noexcept {
  return begin_ts <= read_ts && read_ts < end_ts;
}

// Ordered-set percentile index `ceil(p * n) - 1` clamped to `[0, n - 1]`.
[[nodiscard]] constexpr std::optional<size_t> DiscretePercentileIndex(
    size_t n, uint64_t num, uint64_t den) noexcept {
  if (n == 0 || den == 0) {
    return std::nullopt;
  }
  const uint64_t rank = (static_cast<uint64_t>(n) * num + den - 1U) / den;
  if (rank == 0) {
    return size_t{0};
  }
  const size_t idx = static_cast<size_t>(rank - 1U);
  return idx < n ? idx : (n - 1U);
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_PROPOSED_AND_DB_CORRECTNESS_PROOFS_HPP
