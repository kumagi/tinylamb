import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! FALSE: With only `a.modulus ≤ b.modulus` and compatible residues, `b`'s class need not
refine `a`'s class; divisibility of strides is missing. Counterexample: `a` has stride 2
(class of even numbers), `b` has stride 3 (multiples of 3); `x = 3` satisfies `inside b x`
and lies within `a`'s bounds `[0,10]`, but is not a multiple of 2. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_IntersectionByLargerStride
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-- Refutation of intersecting strides by mere size comparison. -/
theorem refute : ¬ IntersectionByLargerStride.Goal := by
  intro h
  have key := h ⟨0, 10, 2, 0⟩ ⟨0, 10, 3, 0⟩ 3
    (by decide) (by decide) (by decide)
    (by simp only [inside]; decide) (by decide) (by decide)
  exact absurd key (by simp only [inside]; decide)

end TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_IntersectionByLargerStride
