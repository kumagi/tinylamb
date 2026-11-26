import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! FALSE: Sampling only the two endpoints of a table span cannot prove the span constant.
Counterexample: arms `[0,1) ↦ 5` and `[2,3) ↦ 5`; keys 0 and 2 both give `some 5`, but the
interior key 1 matches no arm and gives `none`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_EqualEndpointsProveFlatTable
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- Refutation of endpoint-sampling flatness inference. -/
theorem refute : ¬ EqualEndpointsProveFlatTable.Goal := by
  intro h
  have key := h [⟨0, 1, some 5⟩, ⟨2, 3, some 5⟩] 0 2 1 (by decide) (by decide) (by decide)
  exact absurd key (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_EqualEndpointsProveFlatTable
