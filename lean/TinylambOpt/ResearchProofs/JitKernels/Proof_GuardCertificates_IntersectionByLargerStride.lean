import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.IntersectionByLargerStride.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_GuardCertificates_IntersectionByLargerStride

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.IntersectionByLargerStride

theorem refute : ¬ Goal := by
  intro h
  let a : Domain := { lo := 0, hi := 10, modulus := 2, residue := 0 }
  let b : Domain := { lo := 0, hi := 10, modulus := 3, residue := 2 }
  have hb : inside b (5 : Int) := by unfold inside b; native_decide
  have h' := h a b (5 : Int) (by native_decide) (by native_decide) (by native_decide) hb
    (by native_decide) (by native_decide)
  have hbad : ¬ inside a (5 : Int) := by unfold inside a; native_decide
  exact hbad h'

end Nov26.Proof_GuardCertificates_IntersectionByLargerStride
