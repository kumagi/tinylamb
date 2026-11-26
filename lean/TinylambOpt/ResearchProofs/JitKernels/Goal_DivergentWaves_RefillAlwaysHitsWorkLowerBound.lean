import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_RefillAlwaysHitsWorkLowerBound

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! Greedy refill only balances prefixes of the input order and can exceed
both the longest task and the rounded mean. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.RefillAlwaysHitsWorkLowerBound.Goal := by
  rw [RefillAlwaysHitsWorkLowerBound.Goal]
  intro h
  have hh := h [1,1,2] 2 (by omega)
  have hbad : refill 2 [1,1,2] ≠ max (peak [1,1,2]) (([1,1,2].sum + 2 - 1) / 2) := by
    native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_RefillAlwaysHitsWorkLowerBound
