import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Refutes `FinitePairwiseAccumulators.Goal`: bit-identical horizontal reduction
of two finite partial accumulators fails for the merge order. With
`a = b = 1e308` and `c = -1e308` (all finite), the left order overflows:
`(a + b) = +Inf`, and `+Inf + c = +Inf`; the right order cancels first:
`b + c = +0.0` and `a + 0.0 = 1e308`. The observable bit patterns differ
(`+Inf` vs `1e308`), so `same` fails. All witnesses are closed ground
expressions checked by kernel `decide`. SQL execution must not horizontally
reassociate finite accumulator lanes when a downstream negation would cancel
the intermediate overflow.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 100000
set_option exponentiation.threshold 400
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_FinitePairwiseAccumulators
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ FinitePairwiseAccumulators.Goal := by
  intro h
  have hf : (1e308 : Float).isFinite = true := by decide
  have h' : (((1e308 : Float) + 1e308) + -1e308).toBits
            ≠ (1e308 + (1e308 + -1e308)).toBits := by decide
  exact h' (h 1e308 1e308 (-1e308) hf hf hf)

end TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_FinitePairwiseAccumulators
