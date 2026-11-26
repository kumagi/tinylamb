import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_BiasedResidualThreshold
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-- REFUTED: truncated subtraction drops the borrow. With `base = 5`, `x = 0`,
`c = 3` the left side reads `0 ≤ 3 - 5 = 0` (true) while `5 + 0 ≤ 3` is false,
so the equivalence fails whenever `base > c` and `x = 0`. -/
theorem refute : ¬ BiasedResidualThreshold.Goal := by
  intro h
  exact absurd (h 5 0 3) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_BiasedResidualThreshold
