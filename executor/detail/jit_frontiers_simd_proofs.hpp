/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_JIT_FRONTIERS_SIMD_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_JIT_FRONTIERS_SIMD_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// JitFrontiers SIMD: PrefixKernels, RelationalGuards, SpecializationCache,
// VectorMemory, WordPromises (509 files connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Common_PrefixKernels.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_EvenPairs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_ParitySlope.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_AlternatingDifference_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_DigitPlaces.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_LeadingZeros.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DecimalAppend_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_NeutralBand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_RebiasCount.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_DifferenceFromBase_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_LastValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_OverwriteDoesNotUndoFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_LastPresent_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_ClosedForm.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_MonotoneGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_NonNullCount_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_NonpositiveSkip.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_SingleUpperCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_PositiveFilter_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_MinNegation.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_Product_ZeroSuffix.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_DropHistory.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_MaskedNullBoundary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_ResetOnNull_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_NegativePair.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_NonnegativeRun.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedMagnitude_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_DropMasked.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_ExactEntryGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_FailingLane.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_FinalCheck.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_JoinCertificates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_LateCancellation.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_MergeOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_PeakEnvelope.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_TwoEntryChecks.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels/Goal_SignedSum_UncheckedFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PrefixKernels_All.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_NoNegative.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_AbsLag_ZeroPossible.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_FixedWidth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ClampedDiff_Saturation.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_CancelEqual.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_ExactDelta.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_CorrelatedSub_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_BiasFold.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_DoubleMinus_Translation.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_LeftBranch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_LeastShift_RightBranch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_InactiveZone.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_PositiveExcess_UpperOnlyExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_AxisRectangle.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_InteriorWitness.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ProductCorners_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_ConstantScale.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_NegativeScale.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_ScaledDifference_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_EqualDistance.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_SquareLag_SquareHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_BranchSplit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_DeadEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_DiagonalValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_FoldCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_GuardNecessity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_MinimalHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_OmitOverflow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_PhiHull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_RemoveLower.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards/Goal_WeightedMix_SwapWeight.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/RelationalGuards_All.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Common_SpecializationCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_ThresholdEdge.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_BoundComparison_UnusedScale.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_GuardPriority.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_CheckedAffine_ZeroScale.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_ColdMiss.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_RebindSlot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ConstantLane_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_Collision.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_FreshEpoch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryChecksum_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_EpochIndependent.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_NullOrBadCode.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_DictionaryContents_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_DeadLiteral.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_NullWitness.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_NullPolicy_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_NegativeScaleAlias.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_PowerRemainder_ZeroBits.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_EquivalentSlots.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_MissingSlot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_ProjectPair_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_Flip.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_SignDirection_Tie.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_CanonicalRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_CompileOnce.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_DropLastField.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_EvictWhileHeld.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_KeyMinimal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_OneVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_Period.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_PublishRace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_ReplayBatch.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache/Goal_TruncateKey_Substitute.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/SpecializationCache_All.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Common_VectorMemory.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_FirstZero.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_ReverseSafe.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_BackwardDelta_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_DisjointDistance.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_SafeDirection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ForwardCopy_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_SameCodes.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_UniqueReads.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedGather_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_AllDistinct.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_StableCollisions.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_IndexedScatter_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_InPlaceForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_SharedHalo.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_NeighborSum_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_WordBoundary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_PackedBits_WordCohort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_InPlacePair.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_Reflection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_ReverseInput_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_PeriodicGather.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_RingFootprint.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_RingWindow_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_BroadcastRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_CompressInPlace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideRead_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_ChunkLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_DeadStore.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_DependenceGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_DistinctScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_GatherCache.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_RepeatedAddress.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_SnapshotForward.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_TailExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_TailOverread.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory/Goal_StrideStore_UnorderedScatter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/VectorMemory_All.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/BV64.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_CarryFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_SelfMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_DisjointOr_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_RecoverSource.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_ExactRightShift_WidthCount.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_MaxIncrement.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_OppositeSigns.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedAdd_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_BiasBoundary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_KeyIdentity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedNarrow_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_TimesNegOne.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedProduct_ZeroAnnihilator.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_MinMinusOne.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_SameSign.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_SignedSub_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_AllOnes.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_MaxRoom.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedAdd_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_TopBit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedLeftShift_ZeroCount.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_PowerOfTwo.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedProduct_Unit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_Borrow.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_DeadInputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_DropFlag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_FirstPoison.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_FreezeInvariant.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_GuardExact.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_HoistAcrossPhi.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_SelfZero.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_SpeculateSelect.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises/Goal_UnsignedSub_Unguarded.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/WordPromises_All.lean
// ============================================================================

[[nodiscard]] inline std::vector<int64_t> MaskedPrefixSum(
    std::span<const int64_t> vals, std::span<const uint8_t> mask) {
  std::vector<int64_t> out(vals.size(), 0);
  int64_t acc = 0;
  for (size_t i = 0; i < vals.size(); ++i) {
    if (i < mask.size() && mask[i] != 0) {
      acc += vals[i];
    }
    out[i] = acc;
  }
  return out;
}

[[nodiscard]] constexpr bool AbsLagWithinBand(int64_t a, int64_t b,
                                              int64_t band) noexcept {
  return band >= 0 && a >= b - band && a <= b + band;
}

struct PinnedSpecCache {
  struct Entry {
    uint64_t artifact_id{0};
    size_t pin_count{0};
  };
  std::unordered_map<uint64_t, Entry> entries;

  uint64_t GetOrCompile(uint64_t key, uint64_t next_id) {
    auto [it, inserted] = entries.emplace(key, Entry{.artifact_id = next_id});
    ++it->second.pin_count;
    return it->second.artifact_id;
  }

  void Unpin(uint64_t key) {
    if (const auto it = entries.find(key);
        it != entries.end() && it->second.pin_count > 0) {
      --it->second.pin_count;
    }
  }

  [[nodiscard]] bool TryEvict(uint64_t key) {
    const auto it = entries.find(key);
    if (it == entries.end() || it->second.pin_count > 0) {
      return false;
    }
    entries.erase(it);
    return true;
  }
};

[[nodiscard]] inline std::vector<int64_t> ChunkLocalBackwardDelta(
    std::span<const int64_t> xs, int64_t prev_chunk_tail) {
  std::vector<int64_t> out;
  out.reserve(xs.size());
  int64_t prev = prev_chunk_tail;
  for (const int64_t x : xs) {
    out.push_back(x - prev);
    prev = x;
  }
  return out;
}

[[nodiscard]] constexpr bool DisjointOrCarryFree(uint64_t a,
                                                 uint64_t b) noexcept {
  return (a & b) == 0 && ((a | b) == (a + b));
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_JIT_FRONTIERS_SIMD_PROOFS_HPP
