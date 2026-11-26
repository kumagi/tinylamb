import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RowMajor.Nonoverlap` — A writer may fill distinct bit cells concurrently.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_Nonoverlap

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor

theorem sound_Nonoverlap : RowMajor.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  constructor
  · intro h
    have hw : 0 < w := by omega
    change i * w + a = j * w + b at h
    have hab : a = b := by
      have hmod := congrArg (fun x : Nat => x % w) h
      have hl : (i * w + a) % w = a := by
        simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha]
      have hr : (j * w + b) % w = b := by
        simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hb]
      rw [hl, hr] at hmod
      exact hmod
    subst b
    have hij : i = j := Nat.mul_right_cancel hw (Nat.add_right_cancel h)
    exact ⟨hij, rfl⟩
  · intro h
    rcases h with ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_Nonoverlap
