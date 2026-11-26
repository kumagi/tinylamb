import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! SOUND: For a strided domain `d` and affine map `x ↦ m*x + c`, checking width-fitting at
the first and last realizable values `first d`, `last d` implies every realizable value
`x` fits: `first d ≤ x ≤ last d` and the affine map is monotone in either direction, so the
JIT can eliminate impossible interval endpoints and guard only the extremal products. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_CongruenceTightAffineGuard
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-- Every inside value is at least the rounded first representative. -/
private theorem first_le (d : Domain) {x : Int} (hm : 0 < d.modulus) (hlo : d.lo ≤ x)
    (hc : (x - d.residue) % (d.modulus:Int) = 0) : first d ≤ x := by
  have hmd : (d.modulus:Int) ≠ 0 := by omega
  have ht : (d.residue - d.lo) % (d.modulus:Int) = (x - d.lo) % (d.modulus:Int) := by
    rw [Int.emod_eq_emod_iff_emod_sub_eq_zero]
    have hz : (d.residue - d.lo) - (x - d.lo) = -(x - d.residue) := by omega
    rw [hz, Int.emod_eq_zero_of_dvd (Int.dvd_neg.mpr (Int.dvd_of_emod_eq_zero hc))]
  have hge : 0 ≤ x - d.lo := by omega
  have hdiv : (x - d.lo) % (d.modulus:Int)
      + ((x - d.lo) / (d.modulus:Int)) * (d.modulus:Int) = x - d.lo :=
    Int.emod_add_ediv_mul _ _
  have hqm : 0 ≤ ((x - d.lo) / (d.modulus:Int)) * (d.modulus:Int) :=
    Int.mul_nonneg (Int.ediv_nonneg hge (by omega)) (by omega)
  show d.lo + (d.residue - d.lo) % (d.modulus:Int) ≤ x
  rw [ht]
  omega

/-- Every inside value is at most the rounded last representative. -/
private theorem le_last (d : Domain) {x : Int} (hm : 0 < d.modulus) (hhi : x ≤ d.hi)
    (hc : (x - d.residue) % (d.modulus:Int) = 0) : x ≤ last d := by
  have hmd : (d.modulus:Int) ≠ 0 := by omega
  have ht : (d.hi - d.residue) % (d.modulus:Int) = (d.hi - x) % (d.modulus:Int) := by
    rw [Int.emod_eq_emod_iff_emod_sub_eq_zero]
    have hz : (d.hi - d.residue) - (d.hi - x) = x - d.residue := by omega
    rw [hz, Int.emod_eq_zero_of_dvd (Int.dvd_of_emod_eq_zero hc)]
  have hge : 0 ≤ d.hi - x := by omega
  have hdiv : (d.hi - x) % (d.modulus:Int)
      + ((d.hi - x) / (d.modulus:Int)) * (d.modulus:Int) = d.hi - x :=
    Int.emod_add_ediv_mul _ _
  have hqm : 0 ≤ ((d.hi - x) / (d.modulus:Int)) * (d.modulus:Int) :=
    Int.mul_nonneg (Int.ediv_nonneg hge (by omega)) (by omega)
  have hle : (d.hi - x) % (d.modulus:Int) ≤ d.hi - x := by omega
  show x ≤ d.hi - (d.hi - d.residue) % (d.modulus:Int)
  rw [ht]
  omega

/-- Tight affine guard: endpoint fits certify every inside value. -/
theorem sound : CongruenceTightAffineGuard.Goal := by
  intro d m c w x hm hx hf1 hf2
  obtain ⟨hlo, hhi, hc⟩ := hx
  have hfle : first d ≤ x := first_le d hm hlo hc
  have hxle : x ≤ last d := le_last d hm hhi hc
  simp only [fits] at hf1 hf2 ⊢
  obtain ⟨h1l, h1u⟩ := hf1
  obtain ⟨h2l, h2u⟩ := hf2
  by_cases hmsign : (0:Int) ≤ m
  · have t1 : 0 ≤ m * (x - first d) := Int.mul_nonneg hmsign (by omega)
    have t2 : 0 ≤ m * (last d - x) := Int.mul_nonneg hmsign (by omega)
    rw [Int.mul_sub] at t1 t2
    omega
  · have hn : 0 ≤ -m := by omega
    have t1 : 0 ≤ -m * (x - first d) := Int.mul_nonneg hn (by omega)
    have t2 : 0 ≤ -m * (last d - x) := Int.mul_nonneg hn (by omega)
    rw [Int.neg_mul, Int.mul_sub] at t1
    rw [Int.neg_mul, Int.mul_sub] at t2
    omega

end TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_CongruenceTightAffineGuard
