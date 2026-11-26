import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_Nonoverlap

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

/-- `align 8 w` rounds `w` up to a multiple of eight, hence is at least `w`. -/
private theorem align8_ge (w : Nat) : w ≤ align 8 w := by
  unfold align
  have hdiv := Nat.div_add_mod (w + 7) 8
  rw [Nat.mul_comm 8 ((w + 7) / 8)] at hdiv
  have hmod := Nat.mod_lt (w + 7) (by omega : 0 < 8)
  omega

/-- In `ByteAlignedRow`, bit `b` of row `i` lives at `i * A + b` with
`A = align 8 w`. Because every lane offset is below `A`, reduction mod `A`
recovers the lane, and dividing by the positive `A` recovers the row. -/
theorem sound_Nonoverlap : ByteAlignedRow.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  have hAw : w ≤ align 8 w := align8_ge w
  have haA : a < align 8 w := by omega
  have hbA : b < align 8 w := by omega
  have hApos : 0 < align 8 w := by omega
  constructor
  · intro h
    change i * align 8 w + a = j * align 8 w + b at h
    have hmod := congrArg (fun x : Nat => x % align 8 w) h
    simp [Nat.add_mod, Nat.mod_eq_of_lt haA, Nat.mod_eq_of_lt hbA] at hmod
    subst b
    have hmul : i * align 8 w = j * align 8 w := Nat.add_right_cancel h
    exact ⟨Nat.eq_of_mul_eq_mul_right hApos hmul, rfl⟩
  · intro h
    rcases h with ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_Nonoverlap
