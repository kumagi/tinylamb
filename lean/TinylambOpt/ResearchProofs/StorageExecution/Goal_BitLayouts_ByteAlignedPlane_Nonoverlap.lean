import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_Nonoverlap

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane

/-- `align 8 n` rounds `n` up to a multiple of eight, hence is at least `n`. -/
private theorem align8_ge (n : Nat) : n ≤ align 8 n := by
  unfold align
  have hdiv := Nat.div_add_mod (n + 7) 8
  rw [Nat.mul_comm 8 ((n + 7) / 8)] at hdiv
  have hmod := Nat.mod_lt (n + 7) (by omega : 0 < 8)
  omega

/-- In `ByteAlignedPlane`, bit `b` of row `i` lives at `b * A + i` with
`A = align 8 n`. Every row offset is below `A`, so reduction mod `A`
recovers the row, and cancellation of the common offset recovers the plane. -/
theorem sound_Nonoverlap : ByteAlignedPlane.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  have hnA : n ≤ align 8 n := align8_ge n
  have hiA : i < align 8 n := by omega
  have hjA : j < align 8 n := by omega
  have hApos : 0 < align 8 n := by omega
  constructor
  · intro h
    change a * align 8 n + i = b * align 8 n + j at h
    have hmod := congrArg (fun x : Nat => x % align 8 n) h
    simp [Nat.add_mod, Nat.mod_eq_of_lt hiA, Nat.mod_eq_of_lt hjA] at hmod
    subst j
    have hmul : a * align 8 n = b * align 8 n := Nat.add_right_cancel h
    exact ⟨rfl, Nat.eq_of_mul_eq_mul_right hApos hmul⟩
  · intro h
    rcases h with ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_Nonoverlap
