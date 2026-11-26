import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_OrFoldForRangeMinimum

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.PlanePredicates.OrFoldForRangeMinimum.Goal := by
  intro h
  have hbad : ([1].foldl Nat.land 2) ≠ ((2 :: [1]).min?).getD 0 := by
    native_decide
  exact hbad (h 2 [1])

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_OrFoldForRangeMinimum
