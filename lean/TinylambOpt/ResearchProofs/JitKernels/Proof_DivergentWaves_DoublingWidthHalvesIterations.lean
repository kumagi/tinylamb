import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! REFUTED: Doubling the SIMD width does not in general halve the rounded wave-iteration
count. With lanes of unequal remaining work, a wider wave still waits for its slowest
lane, so the tick total need not shrink at all. Counterexample: `steps = [5, 0]` at
width `1` takes `5` ticks and at width `2` still takes `5` ticks, not `(5+1)/2 = 3`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_DoublingWidthHalvesIterations
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-- Width doubling keeps the same 5 ticks for `[5, 0]`, refuting the halving formula. -/
theorem refute : ¬ DoublingWidthHalvesIterations.Goal := by
  intro h
  have hc := h [5, 0] 1 (by decide)
  exact absurd hc (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_DoublingWidthHalvesIterations
