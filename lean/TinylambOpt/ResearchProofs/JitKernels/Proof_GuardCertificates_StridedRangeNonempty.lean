import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.StridedRangeNonempty.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_GuardCertificates_StridedRangeNonempty

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.StridedRangeNonempty

theorem emod_le_self_of_nonneg {y m : Int} (hy : 0 ≤ y) (hm : 0 < m) : y % m ≤ y := by
  have hdecomp := Int.ediv_mul_add_emod y m
  have hq : 0 ≤ y / m := Int.ediv_nonneg hy (Int.le_of_lt hm)
  have hp : 0 ≤ (y / m) * m := Int.mul_nonneg hq (Int.le_of_lt hm)
  omega

theorem first_congruent (d : Domain) : (first d - d.residue) % (d.modulus : Int) = 0 := by
  unfold first
  let a : Int := d.residue - d.lo
  let m : Int := (d.modulus : Int)
  have hdecomp := Int.ediv_mul_add_emod a m
  rw [Int.mul_comm (a / m) m] at hdecomp
  apply Int.emod_eq_zero_of_dvd
  refine ⟨-((a / m)), ?_⟩
  dsimp [a, m] at hdecomp ⊢
  rw [Int.mul_neg]
  omega

theorem first_ge_lo (d : Domain) (hm : 0 < d.modulus) : d.lo ≤ first d := by
  unfold first
  have hnonneg : 0 ≤ (d.residue - d.lo) % (d.modulus : Int) :=
    Int.emod_nonneg _ (by omega)
  omega

theorem first_le_of_inside (d : Domain) (hm : 0 < d.modulus) {x : Int}
    (hx : inside d x) : first d ≤ x := by
  unfold first
  unfold inside at hx
  let a : Int := d.residue - d.lo
  let m : Int := (d.modulus : Int)
  have hmpos : 0 < m := by dsimp [m]; omega
  have hzero : ((x - d.lo) - a) % m = 0 := by
    have heq : (x - d.lo) - a = x - d.residue := by dsimp [a]; omega
    rw [heq]
    dsimp [m]
    exact hx.2.2
  have hcong : (x - d.lo) % m = a % m := (Int.emod_eq_emod_iff_emod_sub_eq_zero).2 hzero
  have hle := emod_le_self_of_nonneg (by omega : 0 ≤ x - d.lo) hmpos
  dsimp [a, m] at hcong hle ⊢
  omega

theorem last_congruent (d : Domain) : (last d - d.residue) % (d.modulus : Int) = 0 := by
  unfold last
  let a : Int := d.hi - d.residue
  let m : Int := (d.modulus : Int)
  have hdecomp := Int.ediv_mul_add_emod a m
  rw [Int.mul_comm (a / m) m] at hdecomp
  apply Int.emod_eq_zero_of_dvd
  refine ⟨(a / m), ?_⟩
  dsimp [a, m] at hdecomp ⊢
  omega

theorem le_last_of_inside (d : Domain) (hm : 0 < d.modulus) {x : Int}
    (hx : inside d x) : x ≤ last d := by
  unfold last
  unfold inside at hx
  let a : Int := d.hi - d.residue
  let m : Int := (d.modulus : Int)
  have hmpos : 0 < m := by dsimp [m]; omega
  have hzero : ((d.hi - x) - a) % m = 0 := by
    have hz : (x - d.residue) % m = 0 := by dsimp [m]; exact hx.2.2
    -- (hi-x) - (hi-res) = res - x = -(x-res)
    have hdvd : m ∣ ((d.hi - x) - a) := by
      have hdvd1 : m ∣ (x - d.residue) := Int.dvd_of_emod_eq_zero hz
      have hdvd2 : m ∣ -(x - d.residue) := (Int.dvd_neg).2 hdvd1
      have heq : (d.hi - x) - a = -(x - d.residue) := by dsimp [a]; omega
      rw [heq]
      exact hdvd2
    exact Int.emod_eq_zero_of_dvd hdvd
  have hcong : (d.hi - x) % m = a % m := (Int.emod_eq_emod_iff_emod_sub_eq_zero).2 hzero
  have hle := emod_le_self_of_nonneg (by omega : 0 ≤ d.hi - x) hmpos
  dsimp [a, m] at hcong hle ⊢
  omega

theorem sound : Goal := by
  intro d hm
  constructor
  · intro hex
    rcases hex with ⟨x, hx⟩
    have hfirst := first_le_of_inside d hm hx
    exact Int.le_trans hfirst hx.2.1
  · intro hle
    refine ⟨first d, ?_⟩
    unfold inside
    refine ⟨first_ge_lo d hm, hle, ?_⟩
    exact first_congruent d
end Nov26.Proof_GuardCertificates_StridedRangeNonempty
