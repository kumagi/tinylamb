import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_SharedResidueExactDivision

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates


theorem sound : SharedResidueExactDivision.Goal := by
  intro d k x hk hkmod hres hx
  have hmoddiv : (d.modulus : Int) ∣ x - d.residue := Int.dvd_of_emod_eq_zero hx.2.2
  have hkdivmod : (k : Int) ∣ (d.modulus : Int) := Int.ofNat_dvd.mpr hkmod
  have hkdivdiff : (k : Int) ∣ x - d.residue := Int.dvd_trans hkdivmod hmoddiv
  have hkdivres : (k : Int) ∣ d.residue := Int.dvd_of_emod_eq_zero hres
  have hkdivx : (k : Int) ∣ x := by
    rcases hkdivdiff with ⟨a, ha⟩
    rcases hkdivres with ⟨b, hb⟩
    refine ⟨a + b, ?_⟩
    rw [Int.mul_add]
    omega
  exact Int.tdiv_mul_cancel hkdivx
end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_SharedResidueExactDivision
