import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! REFUTED: Persistent lanes do not always finish exactly at the trivial work lower
bound `max(longest task, ceil(total work / width))`. Greedy first-free-lane assignment is
list scheduling, which is within the bound but need not equal it. Counterexample:
`steps = [2, 2, 3]` at width `2`: greedy piles `2 + 3` onto one lane, finishing in `5`
ticks, whereas the lower bound is `max(3, ceil(7/2)) = 4`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_RefillAlwaysHitsWorkLowerBound
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-- Greedy refill takes 5 ticks on `[2, 2, 3]` at width 2, above the bound of 4. -/
theorem refute : ¬ RefillAlwaysHitsWorkLowerBound.Goal := by
  intro h
  have hc := h [2, 2, 3] 2 (by decide)
  exact absurd hc (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_RefillAlwaysHitsWorkLowerBound
