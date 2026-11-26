import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedLeftShift_TopBit

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem rec1 {x : W} : (((x <<< 1) >>> 1) = x) ↔ x.getLsbD 63 = false := by
  rw [BitVec.eq_of_getLsbD_eq_iff]
  constructor
  · intro h
    have h63 := h 63 (by omega)
    rw [BitVec.getLsbD_ushiftRight, BitVec.getLsbD_shiftLeft] at h63
    rw [show (decide (64 < 64)) = false from rfl,
      show (decide (64 < 1)) = false from rfl, Bool.false_and] at h63
    exact h63.symm
  · intro h i hi
    rw [BitVec.getLsbD_ushiftRight, BitVec.getLsbD_shiftLeft]
    rw [show 1 + i - 1 = i from by omega]
    rw [show (!decide (1 + i < (1 : Nat))) = true from by
      rw [decide_eq_false (by omega : ¬ (1 + i < 1))]; rfl]
    rw [Bool.and_true]
    by_cases hlt : 1 + i < 64
    · rw [decide_eq_true hlt, Bool.true_and]
    · rw [decide_eq_false (fun hh => hlt hh), Bool.false_and]
      have : i = 63 := by omega
      rw [this, h]

theorem sound : UnsignedLeftShift.TopBit.Goal := by
  intro x
  dsimp [UnsignedLeftShift.op]
  unfold negative
  rw [show decide ((1:Nat) < 64) = true from by decide, Bool.true_and]
  rw [Bool.beq_eq_decide_eq]
  cases h : x.getLsbD 63 with
  | false => exact decide_eq_true_eq.mpr (rec1.mpr h)
  | true => exact decide_eq_false (mt rec1.mp (by simp [h]))

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedLeftShift_TopBit
