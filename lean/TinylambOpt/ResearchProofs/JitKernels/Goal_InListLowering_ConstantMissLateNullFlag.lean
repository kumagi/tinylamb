import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
A non-NULL probe that is absent from the build list yields NULL exactly when
the build list contains a NULL row, and FALSE otherwise. Therefore the late
NULL flag alone resolves an exact miss; SQL execution may defer checking it
until after the value comparison fails.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_ConstantMissLateNullFlag

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem test_some (xs : Keys) (x : Int) :
    test xs (some x) =
      if some x ∈ xs then some true else if none ∈ xs then none else some false := by
  unfold test
  by_cases h : xs = []
  · simp [h]
  · simp [h]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.InListLowering.ConstantMissLateNullFlag.Goal := by
  intro xs x h
  rw [test_some]
  simp [h]

end TinylambOpt.ResearchProofs.JitKernels.Goal_InListLowering_ConstantMissLateNullFlag