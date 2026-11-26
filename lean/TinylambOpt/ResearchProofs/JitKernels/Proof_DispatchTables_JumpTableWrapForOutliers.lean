import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! FALSE: Masking the biased index modulo the table size without a domain guard dispatches
out-of-domain keys to wrapped slots. Counterexample: one arm `[0,1) ↦ 7` in a size-1 table;
key 2 matches no arm, but `2 % 1 = 0` re-reads slot 0 and returns 7. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_JumpTableWrapForOutliers
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- Refutation of modular wrap-around without a bounds guard. -/
theorem refute : ¬ JumpTableWrapForOutliers.Goal := by
  intro h
  have key := h [⟨0, 1, some 7⟩] 0 1 2 (by decide) (by decide)
  exact absurd key (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_JumpTableWrapForOutliers
