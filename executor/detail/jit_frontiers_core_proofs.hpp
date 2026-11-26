/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_JIT_FRONTIERS_CORE_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_JIT_FRONTIERS_CORE_PROOFS_HPP

#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// JitFrontiers Core: BailoutState, CheckedRewrites, DemandSSA, PackedKeys,
// PredicateBallots (412 files connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/JitFrontiers/BailoutState_All.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_PositiveRoom.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelAdd_UpperCliff.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_LowerCliff.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_NegativeRoom.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_CancelSubtract_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_FinalImpliesInner.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_FinalSite.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DistributeScale_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_LastPositive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_SafeMiddle.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_DoubleHalf_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_CancelledBias.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_SevenTail.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_LinearRebias_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_IdentityElse.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_SingleHole.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_NegateTwice_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_MinusOneCliff.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ProductMinusInput_UnitMultiplier.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_MinFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_SignPair.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_RecenterAbs_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_NegativeMinimum.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_ScaleZero_WidthBound.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_Diagonal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_EndpointCertificate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_FinalOnly.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_FirstFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_FoldValue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_HoistGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_NarrowVersion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_Necessary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_Sufficient.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites/Goal_SquareDistance_SymmetricSuccess.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/CheckedRewrites_All.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Common_DemandSSA.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_FaultWhenUnknown.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_OneLoad.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AbsorbAnd_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_CostCriterion.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_MaskedFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_AndCommute_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_MaskedRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_NullPredicate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CaseAnd_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_ExtraRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_FallbackTrace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_CoalesceDispatch_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_EarlyTrue.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_NegatedTrace.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DeMorgan_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_FaultSlot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_NullRemains.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_DoubleNegation_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_FaultPriority.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_KilledCondition.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_IdenticalArms_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_ColdPath.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_PayloadDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_NestedChoice_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_RepeatedPrefix.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_SkipBoth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_OrFactor_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_DropGuardInFilter.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_FaultOrder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_IgnoreMissing.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_NoNewRead.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_Projection.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_ReadSetGuard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_ReuseBudget.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_TraceParity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_TruthDominates.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA/Goal_TrueGuard_WhereMask.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/DemandSSA_All.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/BackFields_PackedKeys.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Common_PackedKeys.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_FarApart.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_CorrelatedDelta_WideSecond.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_AdjacentNegative.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_RangeAsymmetry.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_DeltaMagnitude_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_ReverseTie.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_MixedDirection_SymmetricEndpoints.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_SignCross.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_OrderBiased_ZeroCorner.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_RejectRemainder.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ScaledKey_Stride.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_LastLocal.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ShardLocal_WrongShard.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_AllOnesKey.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_SignedHalves_UpperCut.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_NullSlot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_SentinelCliff.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_TaggedNull_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_EqualKeys.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_SameHigh.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_XorSide_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_CollisionFree.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_DistinctGroups.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_FallbackBag.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_FoldBoundary.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_GuardRange.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_JoinFanout.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_NearbyNegative.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_Recover.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_ReuseForSort.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Goal_ZigzagPair_SqlNullGate.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Refutations_PackedKeys.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys/Specific_PackedKeys.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PackedKeys_All.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Common_PredicateBallots.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_MaskDifference.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_RejectHit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_AntiPredicate_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_NullEqualsNull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_SelfCompare.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_BoolEqual_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_StickyFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_TotalDefault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Coalesce_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_AllRejected.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ConditionalNull_UnknownSuppresses.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_UnknownNeedsFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Conjunction_ZeroLhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_TrueSuppresses.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Disjunction_UnknownHit.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_EagerNull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_Parity.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_ExclusiveOr_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_MaskNonNull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_IfMissingTest_TwoMissing.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_FalsePremise.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_FaultPremise.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_Implication_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_DisjointOutputs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_EqualNull.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_LimitBeforeFault.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_MaskedTruth.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_MinimalDemand.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_NoUnknown.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_NullOutput.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_SkipRhs.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_SplitActive.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Goal_NullSafeEqual_TrapBallot.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots/Lanes_PredicateBallots.lean
// lean/TinylambOpt/ResearchProofs/JitFrontiers/PredicateBallots_All.lean
// ============================================================================

struct JitBailoutCheckpoint {
  size_t next_row_idx{0};
  bool faulted{false};
};

[[nodiscard]] constexpr bool CheckedAddRangeSafe(int64_t lo, int64_t hi,
                                                 int64_t delta) noexcept {
  if (delta > 0) {
    return hi <= std::numeric_limits<int64_t>::max() - delta;
  }
  if (delta < 0) {
    return lo >= std::numeric_limits<int64_t>::min() - delta;
  }
  return true;
}

[[nodiscard]] constexpr bool DemandSsaGuardNeeded(bool known_false_prefix,
                                                  bool lane_demanded) noexcept {
  return !known_false_prefix && lane_demanded;
}

[[nodiscard]] constexpr std::optional<uint64_t> TryPackKeyPair64(
    uint64_t hi, uint64_t lo, unsigned lo_bits) noexcept {
  if (lo_bits == 0 || lo_bits >= 64) {
    return std::nullopt;
  }
  const uint64_t lo_mask = (uint64_t{1} << lo_bits) - 1U;
  const uint64_t hi_max = (uint64_t{1} << (64U - lo_bits)) - 1U;
  if (lo > lo_mask || hi > hi_max) {
    return std::nullopt;
  }
  return (hi << lo_bits) | lo;
}

[[nodiscard]] constexpr uint64_t AntiPredicateBallot(
    uint64_t valid_mask, uint64_t pred_mask) noexcept {
  return valid_mask & ~pred_mask;
}

[[nodiscard]] constexpr bool BallotLimitReachedBeforeFault(
    uint64_t pass_mask, uint64_t fault_mask, size_t limit) noexcept {
  if (fault_mask == 0) {
    return static_cast<size_t>(std::popcount(pass_mask)) >= limit;
  }
  const unsigned first_fault_bit =
      static_cast<unsigned>(std::countr_zero(fault_mask));
  const uint64_t before_fault =
      first_fault_bit == 0 ? 0ULL : ((uint64_t{1} << first_fault_bit) - 1U);
  return static_cast<size_t>(std::popcount(pass_mask & before_fault)) >= limit;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_JIT_FRONTIERS_CORE_PROOFS_HPP
