import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.SignedDivisionAcrossZero.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_GuardCertificates_SignedDivisionAcrossZero

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.SignedDivisionAcrossZero

theorem refute : ¬ Goal := by
  intro h
  let d : Domain := { lo := -1, hi := 0, modulus := 2, residue := 1 }
  have hd : inside d (-1 : Int) := by unfold inside d; native_decide
  have h' := h d (-1 : Int) 1 (by native_decide) hd
  have hbad : ¬ ((-1 : Int).tdiv (2 : Int) = (-1 : Int) / (2 : Int)) := by native_decide
  exact hbad h'

end Nov26.Proof_GuardCertificates_SignedDivisionAcrossZero
