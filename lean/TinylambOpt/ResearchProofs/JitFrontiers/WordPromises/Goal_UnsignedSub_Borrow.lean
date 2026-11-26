import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `UnsignedSub.Borrow` — `∀ y, 0<y → ir op (0,y)=none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_Borrow

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : UnsignedSub.Borrow.Goal := by
  intro y hy
  unfold ir
  dsimp [UnsignedSub.op]
  have hynz : y.toNat ≠ 0 := by
    intro h0
    have : y = 0 := BitVec.eq_of_toNat_eq h0
    subst y
    simp at hy
  have hnot : ¬ y.toNat ≤ (0 : W).toNat := by
    change ¬ y.toNat ≤ 0
    exact Nat.not_le_of_gt (Nat.pos_of_ne_zero hynz)
  simp
  exact hynz

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_Borrow
