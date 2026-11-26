import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_Nonoverlap

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow

theorem sound_Nonoverlap : SerpentineRow.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  constructor
  · intro h
    have hw : 0 < w := by omega
    let off : Nat → Nat → Nat := fun r x => if r % 2 = 0 then x else w - 1 - x
    change i * w + off i a = j * w + off j b at h
    have hoffa : off i a < w := by
      dsimp [off]
      split <;> omega
    have hoffb : off j b < w := by
      dsimp [off]
      split <;> omega
    have hmod := congrArg (fun x : Nat => x % w) h
    have hl : (i * w + off i a) % w = off i a := by
      simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hoffa]
    have hr : (j * w + off j b) % w = off j b := by
      simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hoffb]
    rw [hl, hr] at hmod
    rw [hmod] at h
    have hij : i = j := Nat.mul_right_cancel hw (Nat.add_right_cancel h)
    subst j
    have hab : a = b := by
      dsimp [off] at hmod
      by_cases hpar : i % 2 = 0
      · simp [hpar] at hmod
        exact hmod
      · simp [hpar] at hmod
        omega
    exact ⟨rfl, hab⟩
  · intro h
    rcases h with ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_Nonoverlap
