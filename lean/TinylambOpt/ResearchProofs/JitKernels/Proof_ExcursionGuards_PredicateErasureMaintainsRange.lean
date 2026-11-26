import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
Deleting terms with a selection mask does NOT preserve a certified excursion
range: the certified scan relied on positive terms that cancelled a negative
drift. Counterexample: guard `[0, 10]`, accumulator 0, deltas `[10, -5, -5]`
with path `[0, 10, 5, 0]` (safe); keeping only the negative terms leaves
`[-5, -5]` with path `[0, -5, -10]`, whose values fall below the lower guard.
Closed ground obligations, checked by `decide`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_PredicateErasureMaintainsRange
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem refute : ¬ PredicateErasureMaintainsRange.Goal := by
  intro h
  have this := h 0 10 0 [10, -5, -5] (fun v => v < 0) (by decide)
  exact absurd this (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_PredicateErasureMaintainsRange
