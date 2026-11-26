import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Refutation: a weaker guard can be a catch-all with a different body. When
the more specific guard also accepts a row, reordering changes the chosen code. -/
namespace Nov26.Proof_VersionDispatch_SpecificitySortDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.SpecificitySortDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.SpecificitySortDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.SpecificitySortDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.SpecificitySortDispatch

theorem refute : ¬ Goal := by
  intro h
  let a : Entry := {mask := 0, value := 0, code := 1}
  let b : Entry := {mask := 0, value := 0, code := 2}
  have hw : weaker a b := by decide
  have hbad := h a b 0 hw
  have hne : dispatch [a, b] 0 ≠ dispatch [b, a] 0 := by decide
  exact hne hbad

end Nov26.Proof_VersionDispatch_SpecificitySortDispatch