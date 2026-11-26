import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.DoublingWidthHalvesIterations.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Refutation: doubling SIMD width does not halve the iteration count after
rounding. Packet maxima are data-dependent; two waves can merge into one wave
whose max is close to the larger input instead of half the total work. -/

namespace Nov26.Proof_DivergentWaves_DoublingWidthHalvesIterations

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.DoublingWidthHalvesIterations

theorem refute : ¬ Goal := by
  intro h
  have hbad := h [1, 2, 3] 1 (by omega)
  have hc : ticks 2 [1, 2, 3] ≠ (ticks 1 [1, 2, 3] + 1) / 2 := by native_decide
  exact hc hbad

end Nov26.Proof_DivergentWaves_DoublingWidthHalvesIterations
