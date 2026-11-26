import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows
set_option linter.unusedSimpArgs false

/-!
An exact reciprocal for the odd part of a divisor also gives the quotient for
the full divisor: integer division associates with multiplication by a power
of two. This lets the SQL kernel peel trailing zero bits once and reuse the
same corrected odd-divisor multiply.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_FixedDivisorPeeling

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem sound : FixedDivisorPeeling.Goal := by
  intro x odd k s hodd hexact
  unfold exactAt at hexact
  unfold guess at hexact
  unfold guess
  rw [hexact]
  rw [Nat.div_div_eq_div_mul]
  rw [Nat.mul_comm]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_FixedDivisorPeeling
