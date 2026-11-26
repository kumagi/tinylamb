import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "the last dividend certifies the range". Exactness of the reciprocal
guess is NOT monotone in the dividend: with `d = 3`, `s = 2`, the largest dividend
`bound = 3` divides exactly (magic = 2: 3*2/4 = 1 = 3/3), but the smaller `x = 2`
already overshoots (2*2/4 = 1 ≠ 0 = 2/3). A generated kernel cannot validate only the
range maximum and skip per-window checks for the dividends below it.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_LastDividendCertifiesRange
open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ LastDividendCertifiesRange.Goal := by
  intro h
  have h1 : exactAt 3 3 2 := rfl
  have h2 := h 3 3 2 (by omega) h1 2 (by omega)
  exact absurd h2 (Nat.succ_ne_zero 0)

end TinylambOpt.ResearchProofs.JitKernels.ReciprocalWindows_LastDividendCertifiesRange
