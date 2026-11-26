import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_Nonoverlap

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane

/-- Equal rotated offsets in the same plane force equal source rows. -/
private theorem rot_mod_injective (n i j a : Nat) (hi : i < n) (hj : j < n)
    (h : (i + a) % n = (j + a) % n) : i = j := by
  by_cases hij_eq : i = j
  · exact hij_eq
  · rcases Nat.lt_or_gt_of_ne hij_eq with hij | hji
    · let q1 := (i + a) / n
      let q2 := (j + a) / n
      let r := (i + a) % n
      have e1 : r + n * q1 = i + a := Nat.mod_add_div (i + a) n
      have e2 : r + n * q2 = j + a := by
        dsimp [r]
        rw [h]
        exact Nat.mod_add_div (j + a) n
      have hq : q1 < q2 := by
        by_cases hq' : q1 < q2
        · exact hq'
        · have hqle : q2 ≤ q1 := Nat.le_of_not_gt hq'
          have hmul : n * q2 ≤ n * q1 := Nat.mul_le_mul_left n hqle
          omega
      have hstep : n * q1 + n ≤ n * q2 := by
        have hs : q1 + 1 ≤ q2 := by omega
        have hh := Nat.mul_le_mul_left n hs
        simpa [Nat.mul_add] using hh
      omega
    · let q1 := (j + a) / n
      let q2 := (i + a) / n
      let r := (j + a) % n
      have e1 : r + n * q1 = j + a := Nat.mod_add_div (j + a) n
      have e2 : r + n * q2 = i + a := by
        dsimp [r]
        rw [← h]
        exact Nat.mod_add_div (i + a) n
      have hq : q1 < q2 := by
        by_cases hq' : q1 < q2
        · exact hq'
        · have hqle : q2 ≤ q1 := Nat.le_of_not_gt hq'
          have hmul : n * q2 ≤ n * q1 := Nat.mul_le_mul_left n hqle
          omega
      have hstep : n * q1 + n ≤ n * q2 := by
        have hs : q1 + 1 ≤ q2 := by omega
        have hh := Nat.mul_le_mul_left n hs
        simpa [Nat.mul_add] using hh
      omega

theorem sound_Nonoverlap : RotatedPlane.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  constructor
  · intro h
    simp only [layout] at h
    have hn : 0 < n := by omega
    have hr1 : (i + a) % n < n := Nat.mod_lt _ hn
    have hr2 : (j + b) % n < n := Nat.mod_lt _ hn
    have hab : a = b := by
      apply Nat.le_antisymm
      · apply Nat.le_of_not_gt
        intro hba
        have hmul : (b + 1) * n ≤ a * n := Nat.mul_le_mul_right n (by omega)
        have hmul' : b * n + n ≤ a * n := by
          simpa [Nat.add_mul] using hmul
        omega
      · apply Nat.le_of_not_gt
        intro hab'
        have hmul : (a + 1) * n ≤ b * n := Nat.mul_le_mul_right n (by omega)
        have hmul' : a * n + n ≤ b * n := by
          simpa [Nat.add_mul] using hmul
        omega
    subst b
    have hmod : (i + a) % n = (j + a) % n := by omega
    exact ⟨rot_mod_injective n i j a hi hj hmod, rfl⟩
  · intro h
    rcases h with ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RotatedPlane_Nonoverlap
