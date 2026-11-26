import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "a narrow low-word residual check certifies the quotient". The wrapped
residual `x + 2^w - (guess x d s * d) mod 2^w` can look like a small valid remainder
while the quotient itself is wrong. For `x = 6`, `d = 7`, `s = 1`, `w = 4`:
magic 7 1 = 1, guess = 6*1/2 = 3, (3*7) mod 16 = 5, residual (6+16-5) mod 16 = 1 < 7,
yet exactAt 6 7 1 fails because 3 ≠ 6/7 = 0. The overestimate comparison cannot be
omitted from the kernel.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_ResidualFromWrappedLowProduct
open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ ResidualFromWrappedLowProduct.Goal := by
  intro h
  have h' := h 6 7 1 4 (by omega) (by decide) (by decide)
  exact absurd h' (Nat.succ_ne_zero 2)

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_ResidualFromWrappedLowProduct
