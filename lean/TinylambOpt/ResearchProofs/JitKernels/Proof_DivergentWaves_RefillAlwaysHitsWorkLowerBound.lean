import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.RefillAlwaysHitsWorkLowerBound.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Refutation: greedy lane refill can exceed the larger of the longest task and
the rounded mean. Load balance is restricted to prefixes of the input order, so
an early long task can keep one lane busy after the other has drained. -/

namespace Nov26.Proof_DivergentWaves_RefillAlwaysHitsWorkLowerBound

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.RefillAlwaysHitsWorkLowerBound

theorem refute : ¬ Goal := by
  intro h
  have hbad := h [1, 1, 2] 2 (by omega)
  have hc : refill 2 [1, 1, 2] ≠ max (peak [1, 1, 2]) (([1, 1, 2].sum + 2 - 1) / 2) := by
    native_decide
  exact hc hbad

end Nov26.Proof_DivergentWaves_RefillAlwaysHitsWorkLowerBound
