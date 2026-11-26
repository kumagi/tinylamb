import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Fixed-divisor peeling: for a divisor `odd * 2^k`, shifting the dividend right by `k`
bits first and then applying the (validated) odd-divisor kernel computes exactly the
same quotient as dividing by the full divisor. This lets the JIT specialize one
reciprocal per odd factor and peel the power-of-two part with a cheap shift, preserving
SQL integer-division results.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_FixedDivisorPeeling
open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem sound : FixedDivisorPeeling.Goal := by
  intro x odd k s hodd h
  have hq : x / 2^k / odd = x / (odd * 2^k) := by
    rw [Nat.div_div_eq_div_mul, Nat.mul_comm (2^k) odd]
  exact h.trans hq

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_FixedDivisorPeeling
