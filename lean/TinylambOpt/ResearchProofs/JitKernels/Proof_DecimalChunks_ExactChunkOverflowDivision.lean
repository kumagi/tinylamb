import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
A nonnegative decimal append `a := a*10^k + b` (with the chunk already
validated, `b < 10^k`) overflows a bound `cap` exactly when the quotient test
`a ≤ (cap - b) / 10^k` fails after `b ≤ cap`. The executor may therefore check
the bound before the multiply, using division instead of risking a wrapped
multiply, with identical accept/reject behavior.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_ExactChunkOverflowDivision
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem sound : ExactChunkOverflowDivision.Goal := by
  intro a b cap k hb
  have hp : 0 < 10^k := by
    have h1 : 1 ≤ 10^k := Nat.one_le_pow _ _ (by decide)
    omega
  constructor
  · intro h
    refine ⟨Nat.le_trans (Nat.le_add_left b (a * 10^k)) h, ?_⟩
    rw [Nat.le_div_iff_mul_le hp]
    omega
  · rintro ⟨h1, h2⟩
    have h3 : a * 10^k ≤ (cap - b) / 10^k * 10^k := Nat.mul_le_mul_right (10^k) h2
    have h4 : (cap - b) / 10^k * 10^k ≤ cap - b := Nat.div_mul_le_self _ _
    omega

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_ExactChunkOverflowDivision
