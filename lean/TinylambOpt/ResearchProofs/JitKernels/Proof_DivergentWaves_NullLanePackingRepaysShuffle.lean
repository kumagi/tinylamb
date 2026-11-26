import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! REFUTED: Compacting zero-work lanes does not always repay its per-packet shuffle
charge. Removing a lone trailing zero shrinks the wave list by one and saves nothing when
the remaining items were already wave-aligned; here the savings vanish entirely.
Counterexample: `steps = [1, 0]` at width `2`: the filtered run still needs 1 tick and
the shuffle charge adds 1, so `1 + 1 ≤ 1` fails. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_NullLanePackingRepaysShuffle
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-- Filtering the zero from `[1, 0]` saves no ticks at width 2; the +1 charge breaks it. -/
theorem refute : ¬ NullLanePackingRepaysShuffle.Goal := by
  intro h
  have hc := h [1, 0] 2 (by decide) (by decide)
  exact absurd hc (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_NullLanePackingRepaysShuffle
