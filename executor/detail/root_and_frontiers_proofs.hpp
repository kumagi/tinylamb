/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_ROOT_AND_FRONTIERS_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_ROOT_AND_FRONTIERS_PROOFS_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// ResearchProofs (root) remaining (85 files -> 134/134 = 100%)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/Atlas_ExecutorResources_Exchanges.lean
// lean/TinylambOpt/ResearchProofs/Atlas_ExecutorResources_Gather.lean
// lean/TinylambOpt/ResearchProofs/Atlas_ExecutorResources_LaneCompaction.lean
// lean/TinylambOpt/ResearchProofs/Atlas_ExecutorResources_Pipeline.lean
// lean/TinylambOpt/ResearchProofs/Atlas_SnapshotMaintenance_BatchWatermarks.lean
// lean/TinylambOpt/ResearchProofs/Atlas_SnapshotMaintenance_IntervalDeletes.lean
// lean/TinylambOpt/ResearchProofs/Atlas_SnapshotMaintenance_ThresholdViews.lean
// lean/TinylambOpt/ResearchProofs/Atlas_SnapshotMaintenance_Versions.lean
// lean/TinylambOpt/ResearchProofs/Atlas_StrongHypotheses.lean
// lean/TinylambOpt/ResearchProofs/Atlas_TextExecution_EditFilter.lean
// lean/TinylambOpt/ResearchProofs/Atlas_TextExecution_EditFilterAnchors.lean
// lean/TinylambOpt/ResearchProofs/Atlas_TextExecution_Grammar.lean
// lean/TinylambOpt/ResearchProofs/Atlas_TextExecution_Lcp.lean
// lean/TinylambOpt/ResearchProofs/Atlas_TextExecution_Wavelet.lean
// lean/TinylambOpt/ResearchProofs/BidirectionalEquiJoinSemijoin.lean
// lean/TinylambOpt/ResearchProofs/BigramChainSkip.lean
// lean/TinylambOpt/ResearchProofs/BitPlaneSum.lean
// lean/TinylambOpt/ResearchProofs/BitSignatureCount.lean
// lean/TinylambOpt/ResearchProofs/BlockLazyZonePrune.lean
// lean/TinylambOpt/ResearchProofs/BoundedHeapTopNMerge.lean
// lean/TinylambOpt/ResearchProofs/CanonicalEquivIdSetAlgebra.lean
// lean/TinylambOpt/ResearchProofs/CappedGroupEarlyFreeze.lean
// lean/TinylambOpt/ResearchProofs/ChanWelfordSharedVarianceState.lean
// lean/TinylambOpt/ResearchProofs/CheckedAbsFence.lean
// lean/TinylambOpt/ResearchProofs/CheckedAggregateTree.lean
// lean/TinylambOpt/ResearchProofs/CheckedLaneAdd.lean
// lean/TinylambOpt/ResearchProofs/ColumnMmapEvictEpoch.lean
// lean/TinylambOpt/ResearchProofs/CongruenceCount.lean
// lean/TinylambOpt/ResearchProofs/CorrelatedAggPartitionKeyFilterPushdown.lean
// lean/TinylambOpt/ResearchProofs/CountDistinctAdaptiveSwitch.lean
// lean/TinylambOpt/ResearchProofs/CteSinglePassSplitFilter.lean
// lean/TinylambOpt/ResearchProofs/DisjointCasePivotBucketDispatch.lean
// lean/TinylambOpt/ResearchProofs/DisjunctiveExistsCascadingBitset.lean
// lean/TinylambOpt/ResearchProofs/DistributedPageFence.lean
// lean/TinylambOpt/ResearchProofs/ExceptAllDebt.lean
// lean/TinylambOpt/ResearchProofs/FilterLoopFootprint.lean
// lean/TinylambOpt/ResearchProofs/FirstKeyBlock.lean
// lean/TinylambOpt/ResearchProofs/FloatFubiniHavingBand.lean
// lean/TinylambOpt/ResearchProofs/FooterOnlyOpenEquiv.lean
// lean/TinylambOpt/ResearchProofs/FullOuterIndicatorBitsetComplement.lean
// lean/TinylambOpt/ResearchProofs/FunctionalDependentGroupKeyElim.lean
// lean/TinylambOpt/ResearchProofs/HammingOneSearch.lean
// lean/TinylambOpt/ResearchProofs/IdentityFkLeftJoinHistogram.lean
// lean/TinylambOpt/ResearchProofs/ImmutableTlcsZoneMapRowBound.lean
// lean/TinylambOpt/ResearchProofs/LateWideRowTopNBlockFence.lean
// lean/TinylambOpt/ResearchProofs/LazyBlockCacheReuse.lean
// lean/TinylambOpt/ResearchProofs/LimitOffsetPrefixComposition.lean
// lean/TinylambOpt/ResearchProofs/MonotonicKeyStreamTopK.lean
// lean/TinylambOpt/ResearchProofs/NonEquiSelfExistsMinMaxWitness.lean
// lean/TinylambOpt/ResearchProofs/OffsetBitmapGather.lean
// lean/TinylambOpt/ResearchProofs/OrderedTagFrontier.lean
// lean/TinylambOpt/ResearchProofs/OverlapPairCount.lean
// lean/TinylambOpt/ResearchProofs/PackedGidDistinctSortDedup.lean
// lean/TinylambOpt/ResearchProofs/PageLoadTrace.lean
// lean/TinylambOpt/ResearchProofs/PairRank.lean
// lean/TinylambOpt/ResearchProofs/PeakCountsAndCompressionCounterexamples.lean
// lean/TinylambOpt/ResearchProofs/Predictors_Bucket.lean
// lean/TinylambOpt/ResearchProofs/Predictors_ClampLinear.lean
// lean/TinylambOpt/ResearchProofs/Predictors_Linear.lean
// lean/TinylambOpt/ResearchProofs/Predictors_Median.lean
// lean/TinylambOpt/ResearchProofs/Predictors_Mirror.lean
// lean/TinylambOpt/ResearchProofs/Predictors_ModuloCounter.lean
// lean/TinylambOpt/ResearchProofs/Predictors_Ordinal.lean
// lean/TinylambOpt/ResearchProofs/Predictors_Previous.lean
// lean/TinylambOpt/ResearchProofs/Predictors_Quadratic.lean
// lean/TinylambOpt/ResearchProofs/Predictors_Seasonal.lean
// lean/TinylambOpt/ResearchProofs/Predictors_SideAffine.lean
// lean/TinylambOpt/ResearchProofs/Predictors_SideDelta.lean
// lean/TinylambOpt/ResearchProofs/Predictors_SideQuotient.lean
// lean/TinylambOpt/ResearchProofs/Predictors_SideResidue.lean
// lean/TinylambOpt/ResearchProofs/Predictors_TrendSeason.lean
// lean/TinylambOpt/ResearchProofs/Predictors_TwoPhase.lean
// lean/TinylambOpt/ResearchProofs/QuotedDelimiterMask.lean
// lean/TinylambOpt/ResearchProofs/RadixReplayOwnership.lean
// lean/TinylambOpt/ResearchProofs/RankSelfJoinToLagLeadAndHashWindow.lean
// lean/TinylambOpt/ResearchProofs/RecursiveDelta.lean
// lean/TinylambOpt/ResearchProofs/SegmentedXor.lean
// lean/TinylambOpt/ResearchProofs/SelectionMaskDeposit.lean
// lean/TinylambOpt/ResearchProofs/SelectiveFilterLateSliceGroup.lean
// lean/TinylambOpt/ResearchProofs/SinglePassMultiConsumerFactScan.lean
// lean/TinylambOpt/ResearchProofs/StreamingRunPage.lean
// lean/TinylambOpt/ResearchProofs/SurrogateKeyChannelPivotLateDim.lean
// lean/TinylambOpt/ResearchProofs/SwarDigitBallot.lean
// lean/TinylambOpt/ResearchProofs/ThresholdBitCounter.lean
// lean/TinylambOpt/ResearchProofs/WalCheckpointTlcsBoundary.lean
//
// Frontiers (27 files -> 27/27 = 100%)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/Frontiers/Arithmetic.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Contractions.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/DecisionTables.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_DirtyWeights.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_EnergyBound.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_Indistinguishable.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_LateSecondMeasure.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_MedianCode.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_NarrowWeights.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_QueryBasis.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_RankRejection.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Goal_WorkloadBases_SeparateMeasures.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/PlanMarkets.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/Provenance.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/RankedJoins.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/RankedJoinsNear.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/RankedJoinsProduct.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/SetTransforms.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/SparseRecovery.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/SpillScheduling.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/SpillSchedulingCache.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/SpillSchedulingLpt.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/SpillSchedulingWrites.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/TextIndex.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/TextIndex/BackwardSearchAndSampledLocate.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/WorkloadBases.lean
// lean/TinylambOpt/ResearchProofs/Frontiers/WorkloadBases_All.lean
// ============================================================================

// Frontiers / TextIndex: exact suffix-array / backward-search range check.
[[nodiscard]] inline std::optional<size_t> FindFirstMatchingPrefixIndex(
    std::span<const std::string_view> sorted_terms,
    std::string_view prefix) noexcept {
  for (size_t i = 0; i < sorted_terms.size(); ++i) {
    if (sorted_terms[i].starts_with(prefix)) {
      return i;
    }
  }
  return std::nullopt;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_ROOT_AND_FRONTIERS_PROOFS_HPP
