import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_AdjacentDelimiterIntersection
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/--
`AdjacentDelimiterIntersection.Goal` claims intersecting two shifted subtract
masks pins an exact adjacent byte pair without candidate verification. This is
false: borrow false positives in either mask admit positions whose lanes are not
the searched pair.
Witness: `x = 0x050405`, `a = 5`, `b = 4`: the intersection reports lanes 0 and 1,
but only lane 0 actually starts the pair (5, 4).
-/
theorem refute : ¬ AdjacentDelimiterIntersection.Goal := by
  intro h
  exact absurd (h 0x050405 5 4) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_AdjacentDelimiterIntersection
