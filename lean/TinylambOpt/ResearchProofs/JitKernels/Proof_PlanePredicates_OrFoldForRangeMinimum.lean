import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_OrFoldForRangeMinimum
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-- REFUTED: a bitwise AND of lanes is not their minimum. `6 &&& 5 = 4` while
`min 6 5 = 5`, so folding `Nat.land` under-approximates the smallest residual. -/
theorem refute : ¬ OrFoldForRangeMinimum.Goal := by
  intro h
  exact absurd (h 6 [5]) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_OrFoldForRangeMinimum
