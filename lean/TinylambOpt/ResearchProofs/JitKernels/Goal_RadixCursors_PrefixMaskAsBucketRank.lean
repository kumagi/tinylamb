import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-! A monotone radix stream can contain several distinct buckets, so the
    same-bucket rank is not the total number of preceding live lanes. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_PrefixMaskAsBucketRank

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.RadixCursors.PrefixMaskAsBucketRank.Goal := by
  intro h
  let xs : List (Option Nat) := [some 1, some 2]
  have hmono : (active xs).Pairwise (fun a b => a ≤ b) := by native_decide
  have hsel : xs[1]? = some (some 2) := by native_decide
  have hh := h xs 1 2 hmono hsel
  have hne : (rank xs 1 2 ≠ (active (xs.take 1)).length) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_PrefixMaskAsBucketRank
