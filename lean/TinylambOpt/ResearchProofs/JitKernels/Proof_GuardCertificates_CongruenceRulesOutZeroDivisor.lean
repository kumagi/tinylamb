import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.CongruenceRulesOutZeroDivisor.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_GuardCertificates_CongruenceRulesOutZeroDivisor

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.CongruenceRulesOutZeroDivisor


theorem sound : Goal := by
  intro d x hm hres hx
  intro hx0
  subst x
  have hz : ((0 : Int) - d.residue) % (d.modulus : Int) = 0 := hx.2.2
  have hdvd1 : (d.modulus : Int) ∣ (0 : Int) - d.residue := Int.dvd_of_emod_eq_zero hz
  have hdvd2 : (d.modulus : Int) ∣ -((0 : Int) - d.residue) := (Int.dvd_neg).2 hdvd1
  have heq : -((0 : Int) - d.residue) = d.residue := by omega
  have hdvd : (d.modulus : Int) ∣ d.residue := by
    rw [← heq]
    exact hdvd2
  exact hres (Int.emod_eq_zero_of_dvd hdvd)
end Nov26.Proof_GuardCertificates_CongruenceRulesOutZeroDivisor
