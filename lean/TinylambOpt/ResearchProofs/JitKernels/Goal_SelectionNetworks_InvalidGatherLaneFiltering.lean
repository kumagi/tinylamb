import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_InvalidGatherLaneFiltering

/-- A suppressed first-stage ordinal shifts the compacted positions, so composing
by ordinal discards the later valid lane. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.InvalidGatherLaneFiltering.Goal := by
  intro h
  have c := h ([7] : List Nat) [5, 0] [0]
  have bad :
      TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.gather
          (TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.gather ([7] : List Nat) [5, 0]) [0] ≠
        TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.gather ([7] : List Nat)
          (TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.gather [5, 0] [0]) := by
    native_decide
  exact bad c

end TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_InvalidGatherLaneFiltering
