import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_SelfZero

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : UnsignedSub.SelfZero.Goal := by
  intro x
  simp [ir, UnsignedSub.op]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_SelfZero
