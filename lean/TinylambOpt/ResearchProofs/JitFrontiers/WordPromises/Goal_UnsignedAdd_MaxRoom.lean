import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedAdd_MaxRoom

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : UnsignedAdd.MaxRoom.Goal := by
  intro x y
  dsimp [UnsignedAdd.op]
  simp only [decide_eq_true_eq]
  show (x.toNat ≤ (x + y).toNat) ↔ (y.toNat ≤ (~~~x).toNat)
  rw [BitVec.toNat_add, BitVec.toNat_not]
  have hx := x.isLt
  have hy := y.isLt
  exact ⟨fun h => by omega, fun h => by omega⟩

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedAdd_MaxRoom
