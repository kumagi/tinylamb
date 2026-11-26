import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedAdd_OppositeSigns

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : SignedAdd.OppositeSigns.Goal := by
  intro x y h
  unfold negative at h
  dsimp [SignedAdd.op]
  unfold negative
  cases hx : x.getLsbD 63 <;> cases hy : y.getLsbD 63 <;> simp_all

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedAdd_OppositeSigns
