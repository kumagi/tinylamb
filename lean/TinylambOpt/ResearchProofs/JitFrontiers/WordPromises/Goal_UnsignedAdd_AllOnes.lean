import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `UnsignedAdd.AllOnes` — `∀ x, x≠0 → ir op (x,0xffffffffffffffff)=none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedAdd_AllOnes

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : UnsignedAdd.AllOnes.Goal := by
  intro x hx
  unfold ir
  dsimp [UnsignedAdd.op]
  have hxpos : 0 < x.toNat := by
    have hne : x.toNat ≠ 0 := by
      intro h
      apply hx
      exact BitVec.eq_of_toNat_eq h
    omega
  have hnot : ¬ x.toNat + (0xffffffffffffffff : W).toNat < 2^64 := by
    change ¬ x.toNat + (2^64 - 1) < 2^64
    exact (Nat.not_lt).mpr (by omega)
  simp
  omega

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedAdd_AllOnes
