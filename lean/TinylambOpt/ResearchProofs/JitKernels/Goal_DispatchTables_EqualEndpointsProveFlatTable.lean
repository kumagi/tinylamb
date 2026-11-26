import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: equal dispatch results at two sampled endpoints do not imply a
flat table between them. A narrow overlapping arm can match an interior key while
both endpoints miss every arm, so collapsing the span changes the CASE result. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_EqualEndpointsProveFlatTable

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem refute : ¬ EqualEndpointsProveFlatTable.Goal := by
  intro h
  let arms : List Arm := [{ lo := 5, hi := 6, result := some 2 }]
  have heq : pick arms 0 = pick arms 6 := by native_decide
  have hbad := h arms 0 6 5 (by omega) (by omega) heq
  have hc : pick arms 5 ≠ pick arms 0 := by native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_EqualEndpointsProveFlatTable
