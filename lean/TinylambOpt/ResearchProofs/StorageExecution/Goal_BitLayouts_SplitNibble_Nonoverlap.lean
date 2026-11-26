import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_Nonoverlap

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble

theorem sound_Nonoverlap : SplitNibble.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  constructor
  · intro h
    by_cases hw : w ≤ 4
    · -- width at most four: every lane is in the low, row-major part
      have hmin : min 4 w = w := Nat.min_eq_right hw
      have ha4 : a < 4 := by omega
      have hb4 : b < 4 := by omega
      have hwpos : 0 < w := by omega
      simp [SplitNibble.layout, hmin, ha4, hb4] at h
      have hab : a = b := by
        have hmod := congrArg (fun x : Nat => x % w) h
        have hl : (i * w + a) % w = a := by
          simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha]
        have hr : (j * w + b) % w = b := by
          simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hb]
        rw [hl, hr] at hmod
        exact hmod
      subst b
      have hij : i = j := Nat.mul_right_cancel hwpos (Nat.add_right_cancel h)
      exact ⟨hij, rfl⟩
    · have hw4 : 4 < w := by omega
      have hmin : min 4 w = 4 := Nat.min_eq_left (by omega)
      have hnpos : 0 < n := by omega
      by_cases ha4 : a < 4
      · by_cases hb4 : b < 4
        · -- both lanes are in the low nibble: tight row-major encoding
          simp [SplitNibble.layout, hmin, ha4, hb4] at h
          have hab : a = b := by
            have hmod := congrArg (fun x : Nat => x % 4) h
            have hl : (i * 4 + a) % 4 = a := by
              simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha4]
            have hr : (j * 4 + b) % 4 = b := by
              simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hb4]
            rw [hl, hr] at hmod
            exact hmod
          subst b
          have hij : i = j := Nat.mul_right_cancel (by decide : 0 < 4) (Nat.add_right_cancel h)
          exact ⟨hij, rfl⟩
        · -- low nibble lives below 4*n, transposed high part at or above 4*n
          simp [SplitNibble.layout, hmin, ha4, hb4] at h
          simp only [Nat.add_assoc] at h
          have hlt : i * 4 + a < n * 4 := by omega
          have hge : n * 4 ≤ n * 4 + ((b - 4) * n + j) := Nat.le_add_right _ _
          have hcontra : i * 4 + a < n * 4 + ((b - 4) * n + j) := by omega
          rw [h] at hcontra
          exact (Nat.lt_irrefl _ hcontra).elim
      · by_cases hb4 : b < 4
        · simp [SplitNibble.layout, hmin, ha4, hb4] at h
          simp only [Nat.add_assoc] at h
          have hlt : j * 4 + b < n * 4 := by omega
          have hge : n * 4 ≤ n * 4 + ((a - 4) * n + i) := Nat.le_add_right _ _
          have hcontra : j * 4 + b < n * 4 + ((a - 4) * n + i) := by omega
          rw [← h] at hcontra
          exact (Nat.lt_irrefl _ hcontra).elim
        · -- both lanes are transposed: subtract the common plane offset,
          -- then use the row index modulo n to recover the lane
          simp [SplitNibble.layout, hmin, ha4, hb4] at h
          simp only [Nat.add_assoc] at h
          have h' : (a - 4) * n + i = (b - 4) * n + j := Nat.add_left_cancel h
          have hmod := congrArg (fun x : Nat => x % n) h'
          have hl : ((a - 4) * n + i) % n = i := by
            simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hi]
          have hr : ((b - 4) * n + j) % n = j := by
            simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hj]
          rw [hl, hr] at hmod
          subst j
          have hsub : a - 4 = b - 4 := Nat.mul_right_cancel hnpos (Nat.add_right_cancel h')
          have hab : a = b := by omega
          exact ⟨rfl, hab⟩
  · intro h
    rcases h with ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_Nonoverlap
