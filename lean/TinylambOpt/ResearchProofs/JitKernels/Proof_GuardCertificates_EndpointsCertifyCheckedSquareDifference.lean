import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! FALSE: Corner evaluations do not certify a fused square-difference `x*(c-x)`.
Counterexample: domain `[0,3]` with stride 1, `c = 4`, width 2 (`fits` range `[-4,4)`);
`first = 0` gives `0*(4-0) = 0` and `last = 3` gives `3*(4-3) = 3`, both fitting, but the
interior vertex `x = 2` gives `2*(4-2) = 4`, which overflows. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_EndpointsCertifyCheckedSquareDifference
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-- Refutation of corner-only certification for the fused quadratic. -/
theorem refute : ¬ EndpointsCertifyCheckedSquareDifference.Goal := by
  intro h
  have key := h ⟨0, 3, 1, 0⟩ 4 2 2 (by decide)
    (by simp only [inside]; decide) (by simp only [fits]; decide)
    (by simp only [fits]; decide)
  exact absurd key (by simp only [fits]; decide)

end TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_EndpointsCertifyCheckedSquareDifference
