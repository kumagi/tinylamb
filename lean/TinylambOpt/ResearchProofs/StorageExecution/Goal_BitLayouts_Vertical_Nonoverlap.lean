import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `Vertical.Nonoverlap` — A writer may fill distinct bit cells concurrently.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_Nonoverlap

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical

theorem sound_Nonoverlap : Vertical.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  constructor
  · intro h
    simp only [layout] at h
    have hn : 0 < n := by omega
    have hab : a = b := by
      by_cases hab' : a = b
      · exact hab'
      · cases Nat.lt_or_gt_of_ne hab' with
        | inl hlt =>
            have hle : (a + 1) * n ≤ b * n := Nat.mul_le_mul_right n (by omega)
            have hsucc : (a + 1) * n = a * n + n := by rw [Nat.add_mul, Nat.one_mul]
            omega
        | inr hgt =>
            have hle : (b + 1) * n ≤ a * n := Nat.mul_le_mul_right n (by omega)
            have hsucc : (b + 1) * n = b * n + n := by rw [Nat.add_mul, Nat.one_mul]
            omega
    subst hab
    refine ⟨?_, rfl⟩
    omega
  · intro h
    cases h with
    | intro hij hab =>
        subst hij
        subst hab
        rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_Nonoverlap
