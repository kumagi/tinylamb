import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_TrimError

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.PageMoments

private theorem sq_nonneg_int (a : Int) : 0 ≤ a ^ 2 := by
  have hsq : a ^ 2 = a * a := by grind
  rw [hsq]
  by_cases h : 0 ≤ a
  · exact Int.mul_nonneg h h
  · have hn : 0 ≤ -a := by omega
    have hmul := Int.mul_nonneg hn hn
    grind

private theorem sum_sq_cauchy (zs : List Int) :
    zs.sum ^ 2 ≤ (zs.length : Int) * (zs.map fun z => z ^ 2).sum := by
  induction zs with
  | nil => simp
  | cons z zs ih =>
      simp only [List.sum_cons, List.length_cons, List.map_cons]
      have hcast : (((zs.length + 1 : Nat) : Int)) = (zs.length : Int) + 1 := by simp
      rw [hcast]
      by_cases hk : zs.length = 0
      · simp [hk]
        grind
      · have hkposNat : 0 < zs.length := Nat.pos_of_ne_zero hk
        have hkpos : (0 : Int) < zs.length := by exact_mod_cast hkposNat
        let k : Int := zs.length
        let Q : Int := (zs.map fun z => z ^ 2).sum
        let S : Int := zs.sum
        have hih : S ^ 2 ≤ k * Q := by simpa [k, Q, S] using ih
        have hv : 0 ≤ k * Q - S ^ 2 := by omega
        have hsq : 0 ≤ (k * z - S) ^ 2 := sq_nonneg_int (k * z - S)
        have hident : (k * z - S) ^ 2 + (k * Q - S ^ 2) =
            k * (k * z ^ 2 - 2 * z * S + Q) := by grind
        have hprod : 0 ≤ k * (k * z ^ 2 - 2 * z * S + Q) := by
          rw [← hident]
          omega
        have hmain : 0 ≤ k * z ^ 2 - 2 * z * S + Q := by
          by_cases hnonneg : 0 ≤ k * z ^ 2 - 2 * z * S + Q
          · exact hnonneg
          · have hneg : k * z ^ 2 - 2 * z * S + Q < 0 := by omega
            have hmulneg : k * (k * z ^ 2 - 2 * z * S + Q) < 0 :=
              Int.mul_neg_of_pos_of_neg (by simpa [k] using hkpos) hneg
            omega
        have hlhs : (z + S) ^ 2 = z ^ 2 + 2 * z * S + S ^ 2 := by grind
        have hrhs : (k + 1) * (z ^ 2 + Q) = z ^ 2 + Q + k * z ^ 2 + k * Q := by grind
        change (z + S) ^ 2 ≤ (k + 1) * (z ^ 2 + Q)
        rw [hlhs, hrhs]
        omega

private theorem energy_partition (s : List Int) (c : Int) (r : Nat) :
    PageMoments.energy s c =
      PageMoments.energy (PageMoments.trimmed s c r) c +
        PageMoments.energy (s.filter fun x => !(x - c).natAbs ≤ r) c := by
  unfold PageMoments.energy PageMoments.trimmed
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      by_cases h : (x - c).natAbs ≤ r
      · simp [h, ih]
        grind
      · simp [h, ih]
        grind

private theorem centered_partition (s : List Int) (c : Int) (r : Nat) :
    s.sum - (s.length : Int) * c =
      ((PageMoments.trimmed s c r).sum - ((PageMoments.trimmed s c r).length : Int) * c) +
        ((s.filter fun x => !(x - c).natAbs ≤ r).map fun x => x - c).sum := by
  unfold PageMoments.trimmed
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      by_cases h : (x - c).natAbs ≤ r
      · simp [h]
        have hleft : x + xs.sum - ((xs.length : Int) + 1) * c =
            (x - c) + (xs.sum - (xs.length : Int) * c) := by grind
        rw [hleft, ih]
        grind
      · simp [h]
        have hleft : x + xs.sum - ((xs.length : Int) + 1) * c =
            (x - c) + (xs.sum - (xs.length : Int) * c) := by grind
        rw [hleft, ih]
        grind

private theorem trimmed_length_add_outside (s : List Int) (c : Int) (r : Nat) :
    (PageMoments.trimmed s c r).length + (s.filter fun x => !(x - c).natAbs ≤ r).length = s.length := by
  unfold PageMoments.trimmed
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      by_cases h : (x - c).natAbs ≤ r
      · simp [h]
        omega
      · simp [h]
        omega

theorem sound_PageMoments_TrimError : PageMoments.TrimError.Goal := by
  intro s c r
  let inside := PageMoments.trimmed s c r
  let outside := s.filter fun x => !(x - c).natAbs ≤ r
  have hE := energy_partition s c r
  have hC := centered_partition s c r
  have hcauchy := sum_sq_cauchy (outside.map fun x => x - c)
  have hlen_in : inside.length + outside.length = s.length := by
    exact trimmed_length_add_outside s c r
  have hlen_eq : s.length - inside.length = outside.length := by
    rw [← hlen_in, Nat.add_sub_cancel_left]
  have hC' : s.sum - (s.length : Int) * c =
      (inside.sum - (inside.length : Int) * c) + (outside.map fun x => x - c).sum := by
    simpa [inside, outside] using hC
  have hsum_zs : (outside.map fun x => x - c).sum =
      (s.sum - (s.length : Int) * c) - (inside.sum - (inside.length : Int) * c) := by
    omega
  have heq_energy : PageMoments.energy s c - PageMoments.energy inside c =
      (outside.map fun x => (x - c) ^ 2).sum := by
    unfold inside
    rw [hE]
    unfold PageMoments.energy
    grind
  change ((s.sum - (s.length : Int) * c) - (inside.sum - (inside.length : Int) * c)) ^ 2 ≤
      ((s.length - inside.length : Nat) : Int) *
        (PageMoments.energy s c - PageMoments.energy inside c)
  rw [← hsum_zs, heq_energy, hlen_eq]
  simpa [Function.comp_def] using hcauchy

end TinylambOpt.ResearchProofs.Atlas.Goal_PageMoments_TrimError
