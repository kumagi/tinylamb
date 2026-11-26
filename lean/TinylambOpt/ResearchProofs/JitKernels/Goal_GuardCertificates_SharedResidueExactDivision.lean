import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! Soundness of shared-residue exact division: divisibility of the modulus and residue by k
transfers to the represented value, so the quotient is exact after integral division. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_SharedResidueExactDivision

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

theorem sound : TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.SharedResidueExactDivision.Goal := by
  intro d k x hk hkdiv hres hinside
  have hdvd_res : (k:Int) ∣ d.residue := Int.dvd_of_emod_eq_zero hres
  have hdvd_mod : (k:Int) ∣ (d.modulus:Int) := by
    exact Int.natCast_dvd_natCast.mpr hkdiv
  have hdvd_sub : (d.modulus:Int) ∣ (x - d.residue) := Int.dvd_of_emod_eq_zero hinside.2.2
  have hdvd_subk : (k:Int) ∣ (x - d.residue) := Int.dvd_trans hdvd_mod hdvd_sub
  have hdvd_x : (k:Int) ∣ x := by
    simpa using Int.dvd_add hdvd_subk hdvd_res
  exact Int.tdiv_mul_cancel hdvd_x

end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_SharedResidueExactDivision
