import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Refutes `SameSignReassociation.Goal`: even when all three values are finite
and strictly positive, reassociation can change the result. With
`a = 9007199254740992.0` (2^53, the first double whose ulp is 2), `b = 1.0`
and `c = 1.0`: the left order computes `(a + 1.0) + 1.0`, where each `+1.0`
hits a round-half-to-even tie back down to 2^53 (the exact midpoints
2^53+1 and 2^53+3 round to even mantissas), giving bit pattern 2^53; the
right order computes `a + (1.0 + 1.0) = a + 2.0 = 9007199254740994.0`
exactly. The bit patterns differ; all witness terms are closed ground
expressions checked by kernel `decide`. Parallel partial sums therefore
cannot be reassociated just because lane signs agree.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_SameSignReassociation
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

private theorem w : (((9007199254740992.0 : Float) + 1.0) + 1.0).toBits
    ≠ (9007199254740992.0 + (1.0 + 1.0)).toBits := by decide

theorem refute : ¬ SameSignReassociation.Goal := by
  intro h
  have hfa : (9007199254740992.0 : Float).isFinite = true := by decide
  have hfb : (1.0 : Float).isFinite = true := by decide
  exact w (h 9007199254740992.0 1.0 1.0 hfa hfb hfb
    (by decide) (by decide) (by decide))

end TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_SameSignReassociation
