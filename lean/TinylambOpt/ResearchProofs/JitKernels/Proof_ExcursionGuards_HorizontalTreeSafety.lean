import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
A safe left-to-right scan of `a ++ b ++ c` does NOT certify the middle
segment `b` started from zero: the left segment can donate a negative offset
that keeps the fused scan in range while `b`'s own prefix excursions, measured
from a zero accumulator, exceed the guard. Counterexample: guard `[-4, 5]`,
`a = [-3]`, `b = [8]`, `c = []`: the fused path is `[0, -3, 5]` (safe), but
`b` alone has path `[0, 8]` and `8 > 5`. Closed ground obligations, checked by
`decide`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_HorizontalTreeSafety
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem refute : ¬ HorizontalTreeSafety.Goal := by
  intro h
  have this := h (-4) 5 0 [-3] [8] [] (by decide) (by decide) (by decide)
  exact absurd this (by decide)

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_HorizontalTreeSafety
