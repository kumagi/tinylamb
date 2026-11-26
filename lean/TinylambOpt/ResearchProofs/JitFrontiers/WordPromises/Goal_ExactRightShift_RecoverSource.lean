import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_ExactRightShift_RecoverSource

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : ExactRightShift.RecoverSource.Goal := by
  intro x k v h
  unfold ir at h
  dsimp [ExactRightShift.op] at h
  simp at h
  rw [← h.2]
  have hklt : k.toNat < 64 := h.1.1
  have hmod : x.toNat % 2 ^ k.toNat = 0 := h.1.2
  apply BitVec.eq_of_toNat_eq
  rw [BitVec.toNat_shiftLeft, BitVec.toNat_ushiftRight]
  simp only [Nat.shiftLeft_eq, Nat.shiftRight_eq_div_pow]
  have hdvd : 2 ^ k.toNat ∣ x.toNat := Nat.dvd_of_mod_eq_zero hmod
  rw [Nat.div_mul_cancel hdvd, Nat.mod_eq_of_lt x.isLt]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_ExactRightShift_RecoverSource
