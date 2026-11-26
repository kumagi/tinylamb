import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! SOUND: When the shared residue class divides out, exact division can replace the guarded
remainder path: if `k > 0`, `k` divides the domain stride and the domain residue is `0`
mod `k`, then every inside value is a multiple of `k`, so `(x.tdiv k) * k = x` — the batch's
tdiv results can be multiplied back exactly. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_SharedResidueExactDivision
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-- Inside values are exact multiples of `k`, so truncating division multiplies back. -/
theorem sound : SharedResidueExactDivision.Goal := by
  intro d k x hk hdvd hres hx
  apply Int.tdiv_mul_cancel
  have hkm : (k:Int) ∣ (d.modulus:Int) := by
    obtain ⟨t, ht⟩ := hdvd
    exact ⟨t, by rw [ht]; rfl⟩
  have hmr : (d.modulus:Int) ∣ x - d.residue := Int.dvd_of_emod_eq_zero hx.2.2
  have hkr : (k:Int) ∣ x - d.residue := Int.dvd_trans hkm hmr
  have hkk : (k:Int) ∣ d.residue := Int.dvd_of_emod_eq_zero hres
  have hx' : x = (x - d.residue) + d.residue := by omega
  rw [hx']
  exact Int.dvd_add hkr hkk

end TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_SharedResidueExactDivision
