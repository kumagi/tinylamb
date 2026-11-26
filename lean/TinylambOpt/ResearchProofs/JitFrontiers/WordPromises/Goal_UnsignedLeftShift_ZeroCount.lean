import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedLeftShift_ZeroCount

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : UnsignedLeftShift.ZeroCount.Goal := by
  intro x
  simp [ir, UnsignedLeftShift.op]
  simpa using x.isLt

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedLeftShift_ZeroCount
