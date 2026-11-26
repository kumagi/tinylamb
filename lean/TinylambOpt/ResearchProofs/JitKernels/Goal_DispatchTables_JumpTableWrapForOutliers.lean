import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: wrapping an out-of-domain CASE key into the jump table fabricates
a match. A key outside every arm must leave the dispatch result unchanged; taking
the offset modulo the table size instead of guarding the domain maps it onto a
real arm. SQL therefore sees a spurious CASE result for an unmatched input. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_JumpTableWrapForOutliers

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DispatchTables.JumpTableWrapForOutliers.Goal := by
  intro h
  have hbad := h [{ lo := 0, hi := 1, result := some 7 }] 0 1 1 (by omega) (by simp)
  have hc : (tab [{ lo := 0, hi := 1, result := some 7 }] 0 1)[((1 : Int) - 0).toNat % 1]?.join ≠
      pick [{ lo := 0, hi := 1, result := some 7 }] 1 := by native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_JumpTableWrapForOutliers
