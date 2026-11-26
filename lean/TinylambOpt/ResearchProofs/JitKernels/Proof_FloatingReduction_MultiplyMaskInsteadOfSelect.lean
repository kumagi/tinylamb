import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Refutes `MultiplyMaskInsteadOfSelect.Goal`: multiplying a finite payload by
positive zero does NOT always yield the canonical `+0.0` inactive-lane bit
pattern. With `x = -1.0` (finite), IEEE gives `x * 0.0 = -0.0`, whose bit
pattern `0x8000000000000000` differs from the pattern of `+0.0`. The witness
is a closed ground expression checked by kernel `decide`. Inactive lanes must
therefore be produced by a select/blending step rather than by `x * 0`
whenever downstream bit comparisons treat signed zeros as distinct.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_MultiplyMaskInsteadOfSelect
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ MultiplyMaskInsteadOfSelect.Goal := by
  intro h
  have h' : ((-1.0 : Float) * 0).toBits ≠ (0 : Float).toBits := by decide
  exact h' (h (-1.0) (show (-1.0 : Float).isFinite = true from by decide))

end TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_MultiplyMaskInsteadOfSelect
