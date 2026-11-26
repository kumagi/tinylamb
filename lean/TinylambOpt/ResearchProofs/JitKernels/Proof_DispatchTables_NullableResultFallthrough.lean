import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-! FALSE: A matched arm whose result code is SQL NULL (`none`) cannot be collapsed into
"no match" when concatenating ladders. Counterexample: a single arm `[0,1) ↦ NULL`; the
joined ladder yields `some none` (matched NULL), while the collapsed right side yields
`none` (no match), so downstream arms with different codes would wrongly fire. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DispatchTables_NullableResultFallthrough
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

/-- Refutation of collapsing a matched NULL result into no-match. -/
theorem refute : ¬ NullableResultFallthrough.Goal := by
  intro h
  have key := h [⟨0, 1, none⟩] [] 0
  exact absurd key (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DispatchTables_NullableResultFallthrough
