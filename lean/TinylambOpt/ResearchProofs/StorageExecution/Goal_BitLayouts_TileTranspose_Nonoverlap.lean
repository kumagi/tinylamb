import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `TileTranspose.Nonoverlap` — A writer may fill distinct bit cells concurrently.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_Nonoverlap

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose

/-- The forward implication: a bit address determines (ordinal, significance). -/
private theorem injective_bits : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb h
  simp only [layout] at h
  have hmod8_i : ((i / 8) * 8 * w + a * 8 + i % 8) % 8 = i % 8 := by
    have h1 : (i / 8) * 8 * w + a * 8 = 8 * ((i / 8) * w + a) := by
      simp [Nat.mul_add, Nat.mul_comm, Nat.mul_left_comm]
    rw [h1, Nat.add_comm, Nat.add_mul_mod_self_left,
      Nat.mod_eq_of_lt (Nat.mod_lt i (by decide))]
  have hmod8_j : ((j / 8) * 8 * w + b * 8 + j % 8) % 8 = j % 8 := by
    have h1 : (j / 8) * 8 * w + b * 8 = 8 * ((j / 8) * w + b) := by
      simp [Nat.mul_add, Nat.mul_comm, Nat.mul_left_comm]
    rw [h1, Nat.add_comm, Nat.add_mul_mod_self_left,
      Nat.mod_eq_of_lt (Nat.mod_lt j (by decide))]
  have hij8 : i % 8 = j % 8 := by
    have := congrArg (· % 8) h
    simpa [hmod8_i, hmod8_j] using this
  have hdiv : (i / 8) * w + a = (j / 8) * w + b := by
    have h1 : (i / 8) * 8 * w + a * 8 + i % 8 =
        8 * ((i / 8) * w + a) + i % 8 := by
      simp [Nat.mul_add, Nat.mul_comm, Nat.mul_left_comm]
    have h2 : (j / 8) * 8 * w + b * 8 + j % 8 =
        8 * ((j / 8) * w + b) + j % 8 := by
      simp [Nat.mul_add, Nat.mul_comm, Nat.mul_left_comm]
    rw [h1, h2, hij8] at h
    omega
  have hw : 0 < w := by omega
  have hab : a = b := by
    have := congrArg (· % w) hdiv
    simpa [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha, Nat.mod_eq_of_lt hb] using this
  subst hab
  have ht : i / 8 = j / 8 := by
    exact Nat.eq_of_mul_eq_mul_right hw (Nat.add_right_cancel hdiv)
  have hi_eq : i = j := by
    have h1 := Nat.div_add_mod i 8
    have h2 := Nat.div_add_mod j 8
    omega
  exact ⟨hi_eq, rfl⟩

theorem sound_Nonoverlap : TileTranspose.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  constructor
  · exact injective_bits n w i j a b hi hj ha hb
  · rintro ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_Nonoverlap
