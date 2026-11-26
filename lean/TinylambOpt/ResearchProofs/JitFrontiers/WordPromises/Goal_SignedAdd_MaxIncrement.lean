import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedAdd_MaxIncrement

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : SignedAdd.MaxIncrement.Goal := by
  change ir SignedAdd.op (0x7fffffffffffffff, 1) = none
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedAdd_MaxIncrement
