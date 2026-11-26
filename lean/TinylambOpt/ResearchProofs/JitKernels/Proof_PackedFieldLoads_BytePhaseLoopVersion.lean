import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
When a full number of lanes spans a whole number of bytes, the packing phase
repeats exactly and the byte stride is the quotient. A loop can therefore
specialize shifts and strides once per period instead of per lane.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PackedFieldLoads_BytePhaseLoopVersion
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

theorem sound : BytePhaseLoopVersion.Goal := by
  intro w i period hp
  unfold bitOffset
  have hp0 : period * w % 8 = 0 := Nat.mod_eq_zero_of_dvd hp
  have hpEq : period * w = 8 * (period * w / 8) := (Nat.mul_div_cancel' hp).symm
  have hpEq' : w * period = 8 * (period * w / 8) := by
    calc w * period = period * w := Nat.mul_comm _ _
      _ = 8 * (period * w / 8) := hpEq
  apply Prod.ext
  · rw [Nat.add_mul, Nat.add_mod, hp0]
    simp
  · rw [Nat.mul_add, hpEq', Nat.mul_comm 8 (period * w / 8)]
    exact Nat.add_mul_div_right (w * i) (period * w / 8) (by decide : 0 < 8)

end Nov26.Proof_PackedFieldLoads_BytePhaseLoopVersion
