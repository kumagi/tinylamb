import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! The stated guard relation points from the broader guard to the more
    specific one, so reordering by it does not preserve first-match dispatch. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_SpecificitySortDispatch

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.SpecificitySortDispatch.Goal := by
  intro h
  let a : Entry := { mask := 0, value := 0, code := 1 }
  let b : Entry := { mask := 1, value := 1, code := 2 }
  have hw : weaker a b = true := by native_decide
  have hh := h a b (1 : Word) hw
  have hne : (dispatch [a,b] (1 : Word) ≠ dispatch [b,a] (1 : Word)) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_SpecificitySortDispatch
