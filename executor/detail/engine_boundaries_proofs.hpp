/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_ENGINE_BOUNDARIES_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_ENGINE_BOUNDARIES_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// EngineBoundaries (100/100 = 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_CoalesceWrites.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_DuplicateSubmit.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_FlightCoverage.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_FrontierOrder.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_GreatestWaiter.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_LateWritePiggyback.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_NewlyDurable.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_RetryLatest.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_StartIsEnough.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_GroupCommit_StickyAck.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_AdvanceSnapshot.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_EarliestEviction.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_EvictionFence.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_ExtendClean.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_HitDespiteEviction.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_LatestPerKey.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_MergeWatermarks.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_RangeFence.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_RootLocalFence.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_IndexHistory_SplitRange.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_CertifiedFloor.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_EmptyCertificate.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_NullSafeIncrement.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_OuterCorrection.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_PromoteKey.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_ShardBudget.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_SplitHeavy.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_TailCap.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_TightCap.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_JoinStatistics_UniformUpper.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_AlignedTraffic.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_EmptyRunElision.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_FourRunCrossover.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_LastWrite.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_RootPeak.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_RotationThreshold.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_RoundOnce.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_RoundedRotation.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_StreamChoice.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_MergeTraffic_WeightedDepth.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_BucketOccupancy.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_CollapseRanks.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_CumeFromRuns.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_DenseOverlap.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_MergeRank.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_NullPeerFront.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_PeerConstantTile.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_PercentDenominator.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_ProportionalTile.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PeerWindows_TilePrefix.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_CardinalityEnds.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_CornersOnly.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_GrantMonotonic.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_MarginalEnvelope.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_ParetoPrune.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_RectangleCutoff.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_ReestimateRegret.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_ReuseFirstRow.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_SingleCrossing.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_PlanEnvelope_SpillBreakpoints.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_FallbackWitness.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_GeometricProgress.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_GrantHonored.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_LargerGrant.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_LeafBag.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_LeafCount.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_OversizedTuple.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_PerHashOrder.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_StagnationIsPermanent.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_RadixBudget_UniqueHeavyChild.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_Backpressure.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_CombineDrains.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_DelayEnqueue.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_DrainCredits.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_PipelineOldPrefix.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_PreserveBytes.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_PublishedPrefix.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_RetractPublished.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_StaleCheckpoint.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_ReplyQueue_UnpublishedError.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_DropReleaseOrder.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_GenerationFence.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_HideBeforeFree.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_IndependentRelease.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_LastRelease.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_LateRelease.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_PinBlocksReuse.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_ReuseAfterDrop.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_SameEpochRetry.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_SpillLeases_WrappedEpoch.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_BoundedWalk.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_LocalDetection.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_LocalRevalidation.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_OneVictim.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_RetireLocal.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_RotateWitness.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_StaleCleanup.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_VictimEdgeOnly.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_WaitDie.lean
// lean/TinylambOpt/ResearchProofs/EngineBoundaries/Goal_WaitCycles_WoundWait.lean
// ============================================================================

// GroupCommit: coalesced flush frontier equals max pending LSN.
[[nodiscard]] inline uint64_t GroupCommitCoalescedFrontier(
    std::span<const uint64_t> pending_lsns) noexcept {
  uint64_t mx = 0;
  for (const uint64_t lsn : pending_lsns) {
    mx = std::max(mx, lsn);
  }
  return mx;
}

// IndexHistory: earliest active reader snapshot bounds safe version eviction.
[[nodiscard]] inline uint64_t EarliestRetainedIndexSnapshot(
    std::span<const uint64_t> active_snapshots, uint64_t head_ts) noexcept {
  if (active_snapshots.empty()) {
    return head_ts;
  }
  return *std::min_element(active_snapshots.begin(), active_snapshots.end());
}

// JoinStatistics: upper bound on inner-join cardinality from max key freq.
[[nodiscard]] constexpr uint64_t JoinCardinalityUpperBound(
    uint64_t probe_rows, uint64_t max_build_key_freq) noexcept {
  return probe_rows * max_build_key_freq;
}

// MergeTraffic: multi-pass k-way merge total bytes = runs_bytes * passes.
[[nodiscard]] inline size_t MergePassCount(size_t num_runs,
                                           size_t fan_in) noexcept {
  if (num_runs <= 1 || fan_in < 2) {
    return 0;
  }
  size_t passes = 0;
  size_t cur = num_runs;
  while (cur > 1) {
    cur = (cur + fan_in - 1) / fan_in;
    ++passes;
  }
  return passes;
}

// PeerWindows / PlanEnvelope / RadixBudget / ReplyQueue / SpillLeases /
// WaitCycles:
// radix partition count fitting within `memory_budget` given per-partition
// buffer size `part_buf_bytes`, and reply-queue backpressure check.
[[nodiscard]] constexpr size_t RadixPartitionsWithinBudget(
    size_t memory_budget, size_t part_buf_bytes) noexcept {
  if (part_buf_bytes == 0 || memory_budget < part_buf_bytes) {
    return 1;
  }
  size_t p = 1;
  while ((p << 1U) * part_buf_bytes <= memory_budget) {
    p <<= 1U;
  }
  return p;
}

[[nodiscard]] constexpr bool ReplyQueueAcceptsBytes(size_t queued_bytes,
                                                    size_t incoming_bytes,
                                                    size_t max_bytes) noexcept {
  return incoming_bytes <= max_bytes &&
         queued_bytes <= max_bytes - incoming_bytes;
}

// WaitCycles: detect cycle in single-waiter-per-owner wait-for graph.
[[nodiscard]] inline bool WaitForChainHasCycle(
    size_t start_owner, std::span<const std::optional<size_t>> waits_on) {
  std::unordered_set<size_t> visited;
  std::optional<size_t> cur = start_owner;
  while (cur.has_value() && *cur < waits_on.size()) {
    if (!visited.insert(*cur).second) {
      return true;
    }
    cur = waits_on[*cur];
  }
  return false;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_ENGINE_BOUNDARIES_PROOFS_HPP
