import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_SignedDivisionAcrossZero

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

theorem refute : ¬ SignedDivisionAcrossZero.Goal := by
  intro h
  let d : Domain := { lo := -1, hi := 0, modulus := 2, residue := 1 }
  have hd : inside d (-1 : Int) := by unfold inside d; native_decide
  have h' := h d (-1 : Int) 1 (by native_decide) hd
  have hbad : ¬ ((-1 : Int).tdiv (2 : Int) = (-1 : Int) / (2 : Int)) := by native_decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_SignedDivisionAcrossZero
