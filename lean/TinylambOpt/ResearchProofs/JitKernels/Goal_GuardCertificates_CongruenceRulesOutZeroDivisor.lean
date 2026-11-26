import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_CongruenceRulesOutZeroDivisor

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates


theorem sound : CongruenceRulesOutZeroDivisor.Goal := by
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
end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_CongruenceRulesOutZeroDivisor
