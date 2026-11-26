import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: a matched CASE arm yielding SQL NULL is not the same as no match.
Joining the result discards that distinction before the fallthrough, so a later
arm's non-NULL result incorrectly replaces the earlier NULL. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_NullableResultFallthrough

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem refute : ¬ NullableResultFallthrough.Goal := by
  intro h
  let b : List Arm := [{ lo := 1, hi := 3, result := none }]
  have hbad := h [] b 1
  have hc : pick ([] ++ b) 1 ≠ ((pick [] 1).join.orElse (fun _ => (pick b 1).join)).map some := by
    native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_NullableResultFallthrough
