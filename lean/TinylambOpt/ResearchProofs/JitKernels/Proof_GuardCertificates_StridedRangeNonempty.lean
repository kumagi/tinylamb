import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! SOUND: A strided domain `d` (bounds plus a congruence class) contains a value iff its
rounded first representative `first d = d.lo + (d.residue - d.lo) % d.modulus` does not
exceed the upper bound. The first representative is the least element of the class that is
`≥ d.lo`, so `first d ≤ d.hi` is a complete compile-time emptiness test for the class,
letting the JIT drop impossible branches. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_StridedRangeNonempty
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-- If `x` is inside `d`, the rounded first representative does not exceed `x`. -/
private theorem first_le_of_inside (d : Domain) {x : Int} (hm : 0 < d.modulus)
    (hlo : d.lo ≤ x) (hc : (x - d.residue) % (d.modulus:Int) = 0) : first d ≤ x := by
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

/-- Nonemptiness of a strided domain equals `first d ≤ d.hi`. -/
theorem sound : StridedRangeNonempty.Goal := by
  intro d hm
  constructor
  · rintro ⟨x, hlo, hhi, hc⟩
    exact Int.le_trans (first_le_of_inside d hm hlo hc) hhi
  · intro hle
    refine ⟨first d, ?_⟩
    have hmd : (d.modulus:Int) ≠ 0 := by omega
    have hn : 0 ≤ (d.residue - d.lo) % (d.modulus:Int) := Int.emod_nonneg _ hmd
    show d.lo ≤ first d ∧ first d ≤ d.hi ∧ (first d - d.residue) % (d.modulus:Int) = 0
    refine ⟨?_, hle, ?_⟩
    · show d.lo ≤ d.lo + (d.residue - d.lo) % (d.modulus:Int)
      omega
    · show (d.lo + (d.residue - d.lo) % (d.modulus:Int) - d.residue) % (d.modulus:Int) = 0
      have hdecomp : (d.residue - d.lo) % (d.modulus:Int)
          + ((d.residue - d.lo) / (d.modulus:Int)) * (d.modulus:Int)
          = d.residue - d.lo := Int.emod_add_ediv_mul _ _
      rw [show d.lo + (d.residue - d.lo) % (d.modulus:Int) - d.residue
            = -(((d.residue - d.lo) / (d.modulus:Int)) * (d.modulus:Int)) from by omega]
      exact Int.emod_eq_zero_of_dvd (Int.dvd_neg.mpr (Int.dvd_mul_left _ _))

end TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_StridedRangeNonempty
