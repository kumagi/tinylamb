import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Refutes `GroupedMeanReciprocal.Goal`: replacing the division in `AVG` by a
precomputed reciprocal is NOT bit-exact in general. With `s = 333.0` and
`count = 49`, the direct quotient `333.0 / 49.0` rounds once, whereas
`333.0 * (1 / 49.0)` rounds `1/49` first and rounds again on the multiply;
for this pair the two results differ in the last bit. All witness terms are
closed ground expressions checked by kernel `decide`. SQL execution may hoist
`AVG` to a reciprocal multiply only when an explicit ULP tolerance is
accepted, not when bit-identical means are required.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_GroupedMeanReciprocal
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ GroupedMeanReciprocal.Goal := by
  intro h
  have hf : (333.0 : Float).isFinite = true := by decide
  have hne : (49 : UInt64) ≠ 0 := by decide
  have h' : (333.0 / (49 : UInt64).toFloat).toBits
            ≠ (333.0 * (1 / (49 : UInt64).toFloat)).toBits := by decide
  exact h' (h 333.0 49 hf hne)

end TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_GroupedMeanReciprocal
