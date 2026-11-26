import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_ExactRightShift_WidthCount

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : ExactRightShift.WidthCount.Goal := by
  intro x
  simp [ir, ExactRightShift.op]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_ExactRightShift_WidthCount
