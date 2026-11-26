import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedProduct_Unit

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : UnsignedProduct.Unit.Goal := by
  intro x
  unfold ir
  dsimp [UnsignedProduct.op]
  simp
  simpa using x.isLt

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedProduct_Unit
