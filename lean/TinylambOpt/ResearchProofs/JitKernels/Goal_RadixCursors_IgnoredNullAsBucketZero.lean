import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

-- Refutation: treating masked lanes as bucket zero changes the base of later buckets.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_IgnoredNullAsBucketZero

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.RadixCursors.IgnoredNullAsBucketZero.Goal := by
  intro h
  have c := h ([none, some 1] : List (Option Nat)) 1 1 (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.RadixCursors.dst
      (([none, some 1] : List (Option Nat)).map (fun x => some (x.getD 0))) 1 =
    TinylambOpt.ResearchGoals.JitKernels.RadixCursors.dst
      ([none, some 1] : List (Option Nat)) 1)) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_IgnoredNullAsBucketZero
