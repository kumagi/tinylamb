import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_ScanWithModularCarry

/-- The endpoint may be in range while an interior prefix is not: base 0 with
deltas `[10,-5]` and width 3 reaches 10 before returning to 5. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.ScanWithModularCarry.Goal := by
  intro h
  let ds : List Int := [10, -5]
  have c := h 0 ds 3 (by native_decide) (by native_decide) (by native_decide)
    10 (by native_decide)
  have bad : ¬ (0 ≤ (10 : Int) ∧ (10 : Int) < (2 : Int) ^ 3) := by native_decide
  exact bad c

end TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_ScanWithModularCarry
