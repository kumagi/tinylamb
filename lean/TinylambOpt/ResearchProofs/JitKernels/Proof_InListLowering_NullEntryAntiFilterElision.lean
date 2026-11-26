import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
Dropping NULL build entries does NOT preserve WHERE NOT IN acceptance when the
probe itself is NULL: against a non-empty original list the probe evaluates to
`none` (unknown), which is not `some false`, but after every entry has been
filtered away the list is empty and the test degenerates to `some false`,
flipping the anti-filter verdict. Counterexample: keys `[NULL]`, probe NULL;
both sides are closed ground terms, so the refutation is checked by `decide`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_NullEntryAntiFilterElision
open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ NullEntryAntiFilterElision.Goal := by
  intro h
  exact absurd (h [none] none) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_NullEntryAntiFilterElision
