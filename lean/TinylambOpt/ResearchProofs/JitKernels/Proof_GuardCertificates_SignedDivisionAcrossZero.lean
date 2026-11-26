import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! FALSE: Truncating division does not equal floor division (arithmetic shift) for a
power-of-two class merely because the stride is `2^k`. Counterexample: domain with stride
`2^1 = 2`, class of odd numbers, `x = -1`: `(-1).tdiv 2 = 0` truncates toward zero, while
`-1 / 2 = -1` floors, and `-1` genuinely lies inside the class. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_SignedDivisionAcrossZero
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-- Refutation of blanket tdiv-to-shift lowering across zero. -/
theorem refute : ¬ SignedDivisionAcrossZero.Goal := by
  intro h
  have key := h ⟨-1, 0, 2, 1⟩ (-1) 1 (by decide) (by simp only [inside]; decide)
  exact absurd key (by decide)

end TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_SignedDivisionAcrossZero
